import chalk from "chalk";
import beautify from "json-beautify";
import dotenv from "dotenv";

const log =
  (chalkColor) =>
  (...msg) =>
    console.log(chalkColor(...msg));

export function splitAndClean(str, sep = ",") {
  if (!str) return null;
  return str.split(sep).filter(Boolean);
}

export const Logger = {
  info: log(chalk.cyanBright),
  debug: log(chalk.blueBright),
  warn: log(chalk.yellow),
  success: log(chalk.greenBright),
  error: log(chalk.redBright),
  json: (msg) => console.log(beautify(msg, null, 4)),
};

export const createContext = (env) => {
  const defaultTopic = env.DEFAULT_TOPIC ?? "first-topic";
  const customTopicText = env.CUSTOM_TOPIC_TEXT ?? "Custom...";
  return {
    defaultTopic,
    brokers: splitAndClean(env.BROKERS) ?? ["localhost:9092"],
    topics: splitAndClean(env.TOPICS)?.concat([customTopicText]) ?? [
      defaultTopic,
      customTopicText,
    ],
    customTopicText,
  };
};

export const createContextFromEnv = () => {
  dotenv.config();
  return createContext(process.env);
};
