import { Kafka, logLevel } from "kafkajs";
import prompts from "prompts";
import { Logger, createContextFromEnv } from "./utils.mjs";

// * Load ENV
const cliContext = createContextFromEnv();

//// * CLI
async function cli() {
  Logger.info("");
  Logger.info("Welcome to Kafka Consumer Client");
  Logger.info("");
  const randId = () => Math.round(Math.random() * 1000).toString();
  const answers = await prompts([
    {
      message:
        "What Kafka Consumer Group ID should I have (leave blank for random)?",
      name: "groupId",
      type: "text",
      initial: randId(),
    },
    {
      message: "What Kafka topic should we receive from?",
      name: "topic",
      type: "select",
      choices: cliContext.topics.map((t) => ({
        title: t,
        value: t,
      })),
      default: cliContext.defaultTopic,
    },
    {
      type: (prev) => (prev === cliContext.customTopicText ? "text" : null),
      message: "Custom topic to start recieving from...",
      name: "topic",
    },
    {
      type: "toggle",
      message: "Should grab from the beginning?",
      name: "grabAllMessages",
      default: false,
    },
  ]);
  Logger.json(answers)
  return answers;
}

//// * Kafka Consumer
// * Basic Kafka setup
const kafka = new Kafka({
  clientId: "hello-consumer",
  brokers: cliContext.brokers,
  logLevel: logLevel.INFO,
});
const SESSION_TIMEOUT = 30 * 1000;

const answers = await cli();
if (Object.values(answers) < 1) {
  process.exit(1);
}
let STOP_GAME = false;
["SIGTERM", "SIGINT", "SIGUSR2"].map((state) =>
  process.once(state, () => (STOP_GAME = true))
);

// * Subscribing from topic
let consumer;
try {
  Logger.info(`Started Consumer with group id: ${answers.groupId}`);
  consumer = kafka.consumer({
    groupId: answers.groupId ?? "test-group",
    allowAutoTopicCreation: false,
    retry: 3,
    sessionTimeout: SESSION_TIMEOUT,
  });

  await consumer.connect();
  await consumer.subscribe({
    topic: answers.topic,
    fromBeginning: answers.grabAllMessages,
  });
  Logger.info("Started listening. To cancel, press CTRL + C.");
  Logger.info("");
  while (!STOP_GAME) {
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
        Logger.info(`- ${prefix} { ${message.key} : ${message.value} }`);
      },
    });
    await new Promise((res) => setTimeout(res, SESSION_TIMEOUT + 10));
  }
} catch (err) {
  Logger.error(err);
} finally {
  consumer?.disconnect();
}

// * Misc Cli.
Logger.info("Thanks for playing");
process.exit();
