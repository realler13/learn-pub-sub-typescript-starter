import amqp from "amqplib";

async function main() {
  console.log("Starting Peril server...");

  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected to RabbitMQ successfully.");

  async function shutdown() {
    console.log("Shutting down Peril server...");
    try {
      await conn.close();
      process.exit(0);
    } catch (err) {
      console.error("Failed to close RabbitMQ connection:", err);
      process.exit(1);
    }
  }

  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
