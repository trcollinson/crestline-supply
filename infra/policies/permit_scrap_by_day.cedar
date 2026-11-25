// Either agent role may read one machine's scrap by day, a month at a
// time, for the five machines that send data (M5). Its own rule, not a
// line in permit_machine_reads, so the read it adds is one file to review.
permit (
  principal,
  action == AgentCore::Action::"PlantData___scrap_by_day",
  resource == AgentCore::Gateway::"${GatewayArn}"
) when {
  (principal == AgentCore::IamEntity::"arn:aws:sts::${AWS::AccountId}:assumed-role/crestline-ops-analyst" ||
   principal == AgentCore::IamEntity::"arn:aws:sts::${AWS::AccountId}:assumed-role/crestline-maintenance-planner") &&
  context.input.days <= 31 &&
  ["LSR-01", "PB-01", "PB-02", "PEM-01", "PC-LINE"].contains(context.input.machine_id)
};
