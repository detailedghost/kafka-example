import { Kafka } from "kafkajs";
import dotenv from "dotenv";
import prompts from "prompts";
import ora from "ora";
import { Logger, createContextFromEnv } from "./utils.mjs";

// * Load ENV
const cliContext = createContextFromEnv();

//// * CLI
const askToKeepPlaying = async () =>
  await prompts([
    {
      message: "Want to send another one?",
      type: "confirm",
      name: "keepPlaying",
      default: true,
    },
  ]);

async function askInput() {
  try {
    const answers = await prompts([
      {
        message: "What Kafka topic should we send to?",
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
        message: "Custom topic to send to...",
        name: "topic",
      },
      {
        message: "Type something to send to a Kafka topic",
        name: "message",
        type: "text",
        validate: (v) => !!v && v.length > 0,
      },
    ]);
    return [answers];
  } catch (err) {
    return [null, err];
  }
}

async function cli(producer) {
  Logger.info("");
  Logger.info("Welcome to Kafka Producer Client");
  Logger.info("");
  let endLoop = false;
  while (!endLoop) {
    const [answers, err] = await askInput();
    if (err) {
      Logger.error(err);
      process.exit(1);
    }
    if (Object.keys(answers) < 1) {
      Logger.error("No answers found.");
      process.exit(1);
    }

    // * Send producer message
    const spinner = ora("Sending message.").start();
    try {
      await producer.send({
        topic: answers.topic,
        messages: [
          {
            key: "from-cli",
            value: answers.message,
          },
        ],
      });
      spinner.succeed("Message sent!");
    } catch (err) {
      spinner.fail("Message had an issue.");
    }
    // *

    endLoop = !(await askToKeepPlaying()).keepPlaying;
  }
}

//// * Create producer

// * Basic Kafka setup
const kafka = new Kafka({
  clientId: "hello-producer",
  brokers: cliContext.brokers,
});

const producer = kafka.producer();
void (await producer.connect());
void (await cli(producer));
void (await producer.disconnect());

// * Misc Cli.
Logger.info("Thanks for playing");
process.exit();
