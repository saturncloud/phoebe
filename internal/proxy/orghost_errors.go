package proxy

// The OpenAI-shaped refusals of the org host and the shared gateway (issue
// #71, tenant-domains design 3.2). No body names the requested model, so no
// refusal is an oracle for which models exist. model_not_found is one fixed
// body per host: a typo and another org's model are the same lookup miss and
// get the same bytes.

func strPtr(s string) *string { return &s }

var (
	orgHostModelNotFound = clientError{
		Message: "This organization has no dedicated endpoint with that name.",
		Type:    "invalid_request_error",
		Code:    "model_not_found",
	}
	orgHostEndpointNotRunning = clientError{
		Message: "This endpoint is stopped. Start it in Token Factory.",
		Type:    "invalid_request_error",
		Code:    "endpoint_not_running",
	}
	orgHostWrongOrganization = clientError{
		Message: "This API key is not valid for this organization's host.",
		Type:    "permission_error",
		Code:    "wrong_organization",
	}
	orgHostConflictingRoute = clientError{
		Message: "This request carries routing headers that do not belong on this host.",
		Type:    "permission_error",
		Code:    "forbidden",
	}
	orgHostUnavailable = clientError{
		Message: "Dedicated endpoints are temporarily unavailable on this host; retry shortly.",
		Type:    "server_error",
		Code:    "service_unavailable",
	}
	orgHostUnknownPath = clientError{
		Message: "Unknown path on this host.",
		Type:    "invalid_request_error",
		Code:    "not_found",
	}
	orgHostVideoEndpointOnly = clientError{
		Message: "This endpoint serves the video API only: POST /v1/videos.",
		Type:    "invalid_request_error",
		Code:    "video_endpoint",
	}
	orgHostForwardLoop = clientError{
		Message: "This request was already forwarded once and cannot be forwarded again.",
		Type:    "server_error",
		Code:    "forward_loop",
	}
	orgHostForwardFailed = clientError{
		Message: "The cluster serving this endpoint could not be reached; retry shortly.",
		Type:    "server_error",
		Code:    "upstream_unavailable",
	}
	modelFieldRequired = clientError{
		Message: "The request body must carry a single model field.",
		Type:    "invalid_request_error",
		Code:    "model_required",
		Param:   strPtr("model"),
	}

	gatewayModelNotFound = clientError{
		Message: "No shared model with that name is available to your organization.",
		Type:    "invalid_request_error",
		Code:    "model_not_found",
	}
	gatewayUnavailable = clientError{
		Message: "The shared gateway is temporarily unavailable; retry shortly.",
		Type:    "server_error",
		Code:    "service_unavailable",
	}
	gatewayNotConfigured = clientError{
		Message: "The shared gateway is not configured on this install.",
		Type:    "server_error",
		Code:    "service_unavailable",
	}
	gatewayForbidden = clientError{
		Message: "This API key is not valid for the shared gateway.",
		Type:    "permission_error",
		Code:    "forbidden",
	}
	gatewayUnknownPath = clientError{
		Message: "Unknown path on this host.",
		Type:    "invalid_request_error",
		Code:    "not_found",
	}
	gatewayAdapterNotLoaded = clientError{
		Message: "The fine-tuned model is still loading; retry shortly.",
		Type:    "server_error",
		Code:    "adapter_not_loaded",
	}
)

// orgHostWrongEndpointHost answers a shared model sent to the org host.
func orgHostWrongEndpointHost(sharedBaseURL string) clientError {
	msg := "This host serves your organization's dedicated endpoints only."
	if sharedBaseURL != "" {
		msg += " Shared models are served at " + sharedBaseURL + "."
	} else {
		msg += " Send shared models to the shared inference host."
	}
	return clientError{Message: msg, Type: "invalid_request_error", Code: "wrong_endpoint_host"}
}

// gatewayWrongEndpointHost answers one of the caller's own org's dedicated
// endpoints sent to the shared host. orgHostURL comes from that endpoint's
// own registry row, so it only ever names the caller's own org host.
func gatewayWrongEndpointHost(orgHostURL string) clientError {
	msg := "This host serves shared models only."
	if orgHostURL != "" {
		msg += " Your organization's dedicated endpoints are served at " + orgHostURL + "."
	} else {
		msg += " Send dedicated endpoints to your organization's dedicated host."
	}
	return clientError{Message: msg, Type: "invalid_request_error", Code: "wrong_endpoint_host"}
}
