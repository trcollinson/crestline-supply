// Either agent role may read margins by brake and shift, a month at a time.
permit (
  principal,
  action == AgentCore::Action::"PlantData___job_margin",
  resource == AgentCore::Gateway::"${GatewayArn}"
) when {
  (principal == AgentCore::IamEntity::"arn:aws:sts::${AWS::AccountId}:assumed-role/crestline-ops-analyst" ||
   principal == AgentCore::IamEntity::"arn:aws:sts::${AWS::AccountId}:assumed-role/crestline-maintenance-planner") &&
  context.input.days <= 31
};
