// Nobody reaches the PLCs through this gateway, whatever any other rule
// says. Default deny already refuses these tools, since nothing permits
// them; this forbid says it out loud and survives someone adding a permit.
forbid (
  principal,
  action in [
    AgentCore::Action::"PlantScada___write_tag",
    AgentCore::Action::"PlantScada___read_tag"
  ],
  resource == AgentCore::Gateway::"${GatewayArn}"
);
