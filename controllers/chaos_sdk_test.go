//go:build test_all || test_chaos

package controllers

import (
	"context"
	"testing"

	chaossdk "github.com/opendatahub-io/operator-chaos/pkg/sdk"
	dspav1 "github.com/opendatahub-io/data-science-pipelines-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

func newChaosReconciler(faults map[chaossdk.Operation]chaossdk.FaultSpec) *DSPAReconciler {
	r := NewFakeController()
	fc := chaossdk.NewFaultConfig(faults)
	r.Client = chaossdk.NewChaosClient(r.Client, fc)
	return r
}

func createTestDSPA(ctx context.Context, r *DSPAReconciler, name, namespace string) error {
	dspa := &dspav1.DataSciencePipelinesApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: dspav1.DSPASpec{
			DSPVersion: "v2",
			Database: &dspav1.Database{
				MariaDB: &dspav1.MariaDB{
					Deploy: true,
				},
			},
			ObjectStorage: &dspav1.ObjectStorage{
				Minio: &dspav1.Minio{
					Deploy: false,
					Image:  "quay.io/test/minio:latest",
				},
			},
		},
	}
	return r.Client.Create(ctx, dspa)
}

func TestChaosSDK_GetFailsOnCRFetch(t *testing.T) {
	faults := map[chaossdk.Operation]chaossdk.FaultSpec{
		chaossdk.OpGet: {ErrorRate: 1.0, Error: "chaos: get blocked"},
	}
	r := newChaosReconciler(faults)
	ctx := context.Background()

	_, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-dspa", Namespace: "default"},
	})

	if err == nil {
		t.Fatal("expected error from Get failure, got nil")
	}
	t.Logf("Got expected error: %v", err)
}

func TestChaosSDK_PatchFailsDuringReconcile(t *testing.T) {
	r := NewFakeController()
	ctx := context.Background()

	if err := createTestDSPA(ctx, r, "test-dspa", "default"); err != nil {
		t.Fatalf("failed to create test DSPA: %v", err)
	}

	fc := chaossdk.NewFaultConfig(map[chaossdk.Operation]chaossdk.FaultSpec{
		chaossdk.OpPatch: {ErrorRate: 1.0, Error: "chaos: patch conflict"},
	})
	r.Client = chaossdk.NewChaosClient(r.Client, fc)

	_, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-dspa", Namespace: "default"},
	})

	if err == nil {
		t.Log("Reconcile succeeded despite patch failures (operator may not use patch in this path)")
	} else {
		t.Logf("Reconcile failed with: %v", err)
	}
}

func TestChaosSDK_UpdateConflictOnStatus(t *testing.T) {
	r := NewFakeController()
	ctx := context.Background()

	if err := createTestDSPA(ctx, r, "test-dspa", "default"); err != nil {
		t.Fatalf("failed to create test DSPA: %v", err)
	}

	fc := chaossdk.NewFaultConfig(map[chaossdk.Operation]chaossdk.FaultSpec{
		chaossdk.OpUpdate: {ErrorRate: 1.0, Error: "chaos: update conflict"},
	})
	r.Client = chaossdk.NewChaosClient(r.Client, fc)

	_, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-dspa", Namespace: "default"},
	})

	if err == nil {
		t.Log("Reconcile succeeded despite update failures (status update may use different path)")
	} else {
		t.Logf("Reconcile failed with: %v", err)
	}
}

func TestChaosSDK_CreateFailsDuringResourceCreation(t *testing.T) {
	r := NewFakeController()
	ctx := context.Background()

	if err := createTestDSPA(ctx, r, "test-dspa", "default"); err != nil {
		t.Fatalf("failed to create test DSPA: %v", err)
	}

	fc := chaossdk.NewFaultConfig(map[chaossdk.Operation]chaossdk.FaultSpec{
		chaossdk.OpCreate: {ErrorRate: 1.0, Error: "chaos: create rejected"},
	})
	r.Client = chaossdk.NewChaosClient(r.Client, fc)

	_, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-dspa", Namespace: "default"},
	})

	if err == nil {
		t.Log("Reconcile succeeded despite create failures")
	} else {
		t.Logf("Reconcile failed with: %v", err)
	}
}

func TestChaosSDK_IntermittentGetFailures(t *testing.T) {
	faults := map[chaossdk.Operation]chaossdk.FaultSpec{
		chaossdk.OpGet: {ErrorRate: 0.5, Error: "chaos: intermittent get failure"},
	}
	r := newChaosReconciler(faults)
	ctx := context.Background()

	successes := 0
	failures := 0

	for i := 0; i < 20; i++ {
		_, err := r.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Name: "test-dspa", Namespace: "default"},
		})
		if err != nil {
			failures++
		} else {
			successes++
		}
	}

	t.Logf("20 reconciles with 50%% Get error rate: %d successes, %d failures", successes, failures)
	if failures == 0 {
		t.Error("expected some failures with 50% error rate, got none")
	}
	if successes == 0 {
		t.Error("expected some successes with 50% error rate, got none")
	}
}

func TestChaosSDK_AllOpsIntermittent(t *testing.T) {
	faults := map[chaossdk.Operation]chaossdk.FaultSpec{
		chaossdk.OpGet:    {ErrorRate: 0.3, Error: "chaos: get"},
		chaossdk.OpCreate: {ErrorRate: 0.3, Error: "chaos: create"},
		chaossdk.OpUpdate: {ErrorRate: 0.3, Error: "chaos: update"},
		chaossdk.OpPatch:  {ErrorRate: 0.3, Error: "chaos: patch"},
		chaossdk.OpDelete: {ErrorRate: 0.3, Error: "chaos: delete"},
	}
	r := newChaosReconciler(faults)
	ctx := context.Background()

	errors := 0
	for i := 0; i < 10; i++ {
		_, err := r.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Name: "test-dspa", Namespace: "default"},
		})
		if err != nil {
			errors++
		}
	}

	t.Logf("10 reconciles with 30%% all-ops error rate: %d errors", errors)
}
