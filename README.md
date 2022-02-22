# kafka-example
A simple kafka example.

# steps to run
1. Run `docker-compose up -d` to start the Kafka docker instance
2. Run `npm i` / `yarn i` / `pnpm i` to install dependencies.
  - Remember to remove the `pnpm-lock.yml` file if you plan on using anything other than `pnpm`
3. Run `node producer.mjs` and add a value
4. In a separate window, run `node consumer.mjs` to start a new consumer and connect to same topic


