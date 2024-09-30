# @lichtblick/ros1

> _Standalone TypeScript implementation of the ROS 1 (Robot Operating System) protocol with a pluggable transport layer_

[![npm version](https://img.shields.io/npm/v/@lichtblick/ros1.svg?style=flat)](https://www.npmjs.com/package/@lichtblick/ros1)

## Usage

```Typescript
// An example client that connects to the ROS1 turtlesim node

import { RosNode } from "@lichtblick/ros1";
import { getEnvVar, getHostname, getNetworkInterfaces, getPid, TcpSocketNode } from "@lichtblick/ros1/nodejs";
import { HttpServerNodejs } from "@lichtblick/xmlrpc/nodejs";

async function main() {
  const name = "/testclient";
  let rosNode: RosNode | undefined;

  try {
    rosNode = new RosNode({
      name,
      rosMasterUri: getEnvVar("ROS_MASTER_URI") ?? "http://localhost:11311/",
      hostname: RosNode.GetRosHostname(getEnvVar, getHostname, getNetworkInterfaces),
      pid: getPid(),
      httpServer: new HttpServerNodejs(),
      tcpSocketCreate: TcpSocketNode.Create,
      log: console,
    });

    await rosNode.start();

    const params = await rosNode.subscribeAllParams();
    console.dir(params);

    const sub = rosNode.subscribe({
      topic: "/turtle1/color_sensor",
      dataType: "turtlesim/Color",
    });

    sub.on("message", (msg, data, pub) => {
      console.log(
        `[MSG] ${JSON.stringify(msg)} (${
          data.byteLength
        } bytes from ${pub.connection.getTransportInfo()})`,
      );
    });

    await new Promise((resolve) => setTimeout(resolve, 1000));

    console.dir(sub.getStats());
  } catch (err) {
    const msg = (err as Error).stack ?? `${err}`;
    console.error(msg);
  } finally {
    rosNode?.shutdown();
  }
}

void main();
```

### Test

`yarn test`

## License

@lichtblick/ros1 is licensed under the [MIT License](https://opensource.org/licenses/MIT).

## Releasing

1. Run `yarn version --[major|minor|patch]` to bump version
2. Run `git push && git push --tags` to push new tag
3. GitHub Actions will take care of the rest
