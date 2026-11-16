import { randomUUID } from 'node:crypto';
import { encode, decode } from '@msgpack/msgpack';
import { Request, Subscriber } from 'zeromq';

const name = process.env.BOT_NAME || 'bot-1';
const broker = process.env.BROKER_ENDPOINT || 'tcp://broker:5555';
const pubsub = process.env.PUBSUB_ENDPOINT || 'tcp://pubsub-proxy:5558';
const messageInterval = Number(process.env.BOT_MESSAGE_INTERVAL_SECONDS || 1) * 1000;
const retryInterval = Number(process.env.BOT_RETRY_INTERVAL_SECONDS || 2) * 1000;
const messagesPerBatch = Number(process.env.BOT_MESSAGES_PER_BATCH || 10);
const minChannels = Number(process.env.BOT_MIN_CHANNELS || 5);
const minSubscriptions = Number(process.env.BOT_MIN_SUBSCRIPTIONS || 3);

let lamport = 0;
let channelNumber = 1;
const subscriptions = new Set();
const subscriber = new Subscriber();

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const pick = (items) => items[Math.floor(Math.random() * items.length)];

function envelope(action, fields = {}) {
  lamport += 1;
  return { action, ...fields, timestamp: Date.now(), lamport, request_id: randomUUID() };
}

function observe(message) {
  lamport = Math.max(lamport, message.lamport);
}

async function request(action, fields = {}) {
  // A new REQ socket avoids an invalid request/reply state after a timeout.
  const socket = new Request({ receiveTimeout: 5000, sendTimeout: 5000 });
  socket.connect(broker);
  const outgoing = envelope(action, { username: name, ...fields });
  try {
    console.log(`[${name}] SENT ${JSON.stringify(outgoing)}`);
    await socket.send(encode(outgoing));
    const [data] = await socket.receive();
    const incoming = decode(data);
    observe(incoming);
    console.log(`[${name}] RECEIVED ${JSON.stringify(incoming)}`);
    return incoming;
  } finally {
    socket.close();
  }
}

async function listen() {
  for await (const [topic, body] of subscriber) {
    const incoming = decode(body);
    observe(incoming);
    console.log(`[${name}] CHANNEL ${topic.toString()}: ${JSON.stringify({
      ...incoming, received_at: Date.now(),
    })}`);
  }
}

async function updateChannels() {
  const response = await request('LIST_CHANNELS');
  const channels = response.channels || [];
  if (channels.length < minChannels) {
    const channel = `${name}-channel-${channelNumber++}`;
    const created = await request('CREATE_CHANNEL', { channel });
    if (created.status === 'ok') channels.push(channel);
  }

  const available = channels.filter((channel) => !subscriptions.has(channel));
  if (subscriptions.size < minSubscriptions && available.length) {
    const channel = pick(available);
    subscriber.subscribe(channel);
    subscriptions.add(channel);
    console.log(`[${name}] SUBSCRIBED ${channel}`);
  }
  return channels;
}

async function main() {
  subscriber.connect(pubsub);
  listen().catch((error) => console.error(`[${name}] Subscriber: ${error.message}`));
  while (true) {
    try {
      const login = await request('LOGIN');
      if (login.status !== 'ok') throw new Error(login.error || 'Login failed');
      break;
    } catch (error) {
      console.error(`[${name}] Login: ${error.message}`);
      await sleep(retryInterval);
    }
  }

  while (true) {
    try {
      const channels = await updateChannels();
      for (let i = 0; i < messagesPerBatch; i += 1) {
        if (!channels.length) break;
        await request('PUBLISH', {
          channel: pick(channels),
          content: `Message ${i + 1} from ${name} (${randomUUID().slice(0, 8)})`,
        });
        await sleep(messageInterval);
      }
    } catch (error) {
      console.error(`[${name}] Request: ${error.message}`);
      await sleep(retryInterval);
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});