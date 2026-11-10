// The core owns registry parsing, selector expansion and candidate selection.
export default function createPlugin(config: any, context: any) {
  return context.api.createTeamRegistryRouterPlugin({
    projectRoot: context.projectRoot,
    registryPath: config.registryPath ?? 'team.example.yaml',
    networkId: config.networkId ?? 'local',
    name: 'example-team-router',
    priority: 50,
  });
}
