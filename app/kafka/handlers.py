EVENT_HANDLERS: dict[str, str] = {
    "OrderCreated": "Your order has been placed",
    "PaymentAuthorized": "Payment confirmed",
    "PaymentFailed": "Payment failed: {reason}",
    "RestaurantAccepted": "Restaurant is preparing your order (~{estimated_prep_time} min)",
    "CourierAssigned": "Courier {courier_name} is on the way!",
    "DeliveryCompleted": "Order delivered!",
    "RestaurantRejected": "Order rejected by restaurant: {reason}",
    "CourierAssignmentFailed": "No courier available: {reason}",
    "PaymentRefunded": "Your payment has been refunded: {reason}",
}
