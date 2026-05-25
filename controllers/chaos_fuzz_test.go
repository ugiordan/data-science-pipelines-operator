//go:build test_all || test_chaos

package controllers

import (
	"context"
	"testing"

	chaossdk "github.com/opendatahub-io/operator-chaos/pkg/sdk"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

func FuzzDSPAReconciler(f *testing.F) {
	f.Add(uint8(0), uint8(0), uint8(50))
	f.Add(uint8(1), uint8(1), uint8(100))
	f.Add(uint8(3), uint8(2), uint8(30))
	f.Add(uint8(7), uint8(0), uint8(75))

	f.Fuzz(func(t *testing.T, opMask uint8, faultType uint8, intensity uint8) {
		if intensity == 0 {
			return
		}

		errorRate := float64(intensity) / 100.0
		if errorRate > 1.0 {
			errorRate = 1.0
		}

		ops := []chaossdk.Operation{
			chaossdk.OpGet,
			chaossdk.OpCreate,
			chaossdk.OpUpdate,
			chaossdk.OpPatch,
			chaossdk.OpDelete,
			chaossdk.OpList,
		}

		faults := make(map[chaossdk.Operation]chaossdk.FaultSpec)
		for i, op := range ops {
			if opMask&(1<<i) != 0 {
				faults[op] = chaossdk.FaultSpec{
					ErrorRate: errorRate,
					Error:     "fuzz-injected failure",
				}
			}
		}

		if len(faults) == 0 {
			return
		}

		r := NewFakeController()
		ctx := context.Background()

		if err := createTestDSPA(ctx, r, "fuzz-dspa", "default"); err != nil {
			return
		}

		fc := chaossdk.NewFaultConfig(faults)
		r.Client = chaossdk.NewChaosClient(r.Client, fc)

		r.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Name: "fuzz-dspa", Namespace: "default"},
		})
	})
}
