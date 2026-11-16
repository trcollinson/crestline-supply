// Either agent role may look up one job: it is one row, whatever is asked.
permit (
  principal,
  action == AgentCore::Action::"PlantData___job_status",
  resource == AgentCore::Gateway::"${GatewayArn}"
) when {
  principal == AgentCore::IamEntity::"arn:aws:sts::${AWS::AccountId}:assumed-role/crestline-ops-analyst" ||
  principal == AgentCore::IamEntity::"arn:aws:sts::${AWS::AccountId}:assumed-role/crestline-maintenance-planner"
};
