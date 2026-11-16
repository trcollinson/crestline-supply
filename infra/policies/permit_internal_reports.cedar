// The ops analyst may send a report, to a plant address only. The match is
// on the whole address, case and all: `ops@crestline.example.evil.com`
// doesn't end in `@crestline.example`, and `OPS@CRESTLINE.EXAMPLE` is
// refused too, which is the safe way to be wrong.
permit (
  principal == AgentCore::IamEntity::"arn:aws:sts::${AWS::AccountId}:assumed-role/crestline-ops-analyst",
  action == AgentCore::Action::"Reports___send_report",
  resource == AgentCore::Gateway::"${GatewayArn}"
) when {
  context.input.recipient like "*@crestline.example"
};
