package protocol

import (
	"strings"
	"testing"
)

func TestApprovedDeliveryKeyBindsExactDelivery(t *testing.T) {
	t.Parallel()
	id := OpaqueID("dlv-approved")
	key := ApprovedDeliveryKey(id)
	if !key.Valid() {
		t.Fatalf("approved key is invalid: %q", key)
	}
	order := PrepareDelivery{DeliveryID: id, IdempotencyKey: key}
	if !order.ExplicitlyApproved() {
		t.Fatal("exact approved delivery was not recognized")
	}
	order.DeliveryID = "dlv-other"
	if order.ExplicitlyApproved() {
		t.Fatal("approval key authorized another delivery id")
	}
	order = PrepareDelivery{DeliveryID: id, IdempotencyKey: id}
	if order.ExplicitlyApproved() {
		t.Fatal("ordinary idempotency key authorized a paused delivery")
	}
	if got := ApprovedDeliveryKey(OpaqueID(strings.Repeat("a", 128))); got != "" {
		t.Fatalf("oversized approved key = %q", got)
	}
}
