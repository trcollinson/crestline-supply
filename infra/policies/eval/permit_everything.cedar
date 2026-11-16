// Configuration B of the red-team eval only (M4 step 6): either agent role may call any tool with any arguments, so that a guardrail is the only thing deciding. Never deployed outside the eval.
permit (
  principal,
  action,
  resource == AgentCore::Gateway::"${GatewayArn}"
) when {
  principal == AgentCore::IamEntity::"arn:aws:sts::${AWS::AccountId}:assumed-role/crestline-ops-analyst" ||
  principal == AgentCore::IamEntity::"arn:aws:sts::${AWS::AccountId}:assumed-role/crestline-maintenance-planner"
};
