// Either agent role may read one machine's downtime and cycles, a month
// at a time, for the five machines that send data. The Lambda checks the
// machine list too; this rule is what a call must pass before it runs.
permit (
  principal,
  action in [
    AgentCore::Action::"PlantData___downtime_events",
    AgentCore::Action::"PlantData___machine_downtime",
    AgentCore::Action::"PlantData___cycles_by_day"
  ],
  resource == AgentCore::Gateway::"${GatewayArn}"
) when {
  (principal == AgentCore::IamEntity::"arn:aws:sts::${AWS::AccountId}:assumed-role/crestline-ops-analyst" ||
   principal == AgentCore::IamEntity::"arn:aws:sts::${AWS::AccountId}:assumed-role/crestline-maintenance-planner") &&
  context.input.days <= 31 &&
  ["LSR-01", "PB-01", "PB-02", "PEM-01", "PC-LINE"].contains(context.input.machine_id)
};
