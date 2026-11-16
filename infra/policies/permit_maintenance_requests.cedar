// The maintenance planner may file routine maintenance requests. An
// emergency, or anything about safety, needs a person on the phone, not
// an agent. (Cedar can't measure a string, so the description's length is
// capped by the Lambda alone: 500 characters.)
permit (
  principal == AgentCore::IamEntity::"arn:aws:sts::${AWS::AccountId}:assumed-role/crestline-maintenance-planner",
  action == AgentCore::Action::"Maintenance___create_maintenance_request",
  resource == AgentCore::Gateway::"${GatewayArn}"
) when {
  ["low", "normal"].contains(context.input.priority) &&
  context.input.category != "safety"
};
