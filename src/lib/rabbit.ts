// src/lib/rabbit.ts
import 'server-only';
import type { Connection, Channel, ConsumeMessage } from 'amqplib';

/* ---------- Config ---------- */
const RABBIT_URL = process.env.RABBIT_URL ?? 'amqp://guest:guest@localhost:5672';
const JOBS_QUEUE = process.env.JOBS_QUEUE ?? 'img_jobs';
const RESULTS_EX = process.env.RESULTS_EX ?? 'img.results';

/* ---------- Lazy-load amqplib (ESM/CJS safe) ---------- */
type AMQPModule = typeof import('amqplib');
let _amqpMod: AMQPModule | null = null;

async function loadAmqp(): Promise<AMQPModule> {
  if (_amqpMod) return _amqpMod;
  const mod: any = await import('amqplib');
  // חלק מהבאנדלים שמים connect על default
  const picked = mod?.connect ? mod : mod?.default;
  if (!picked?.connect) throw new Error('amqplib.connect not found (ESM/CJS mismatch)');
  _amqpMod = picked as AMQPModule;
  return _amqpMod!;
}

/* ---------- Singletons (שרת בלבד, שורד Hot-Reload) ---------- */
let _conn: Connection | null = null;
let _sharedCh: Channel | null = null;

async function getConnection(): Promise<Connection> {
  if (_conn) return _conn;
  const amqp = await loadAmqp();
  // ליהוק ל-any עוקף חוסר תאימות טיפוסים בין ESM/CJS בסביבת Next
  _conn = (await amqp.connect(RABBIT_URL)) as any;
  return _conn as any;
}

export async function getChannel(): Promise<Channel> {
  if (_sharedCh) return _sharedCh;
  const c = await getConnection();
  const ch = (await (c as any).createChannel()) as Channel;
  await ch.assertQueue(JOBS_QUEUE, { durable: true });
  await ch.assertExchange(RESULTS_EX, 'direct', { durable: true });
  _sharedCh = ch;
  return ch;
}

/* ---------- Producer: שליחת Job לעיבוד ---------- */
export async function sendJob(job: unknown): Promise<void> {
  const ch = await getChannel();
  ch.sendToQueue(JOBS_QUEUE, Buffer.from(JSON.stringify(job)), {
    persistent: true,
    contentType: 'application/json',
  });
}

/* ---------- Subscribe רציף לתוצאות לפי chatId (מוחזר unsubscribe) ---------- */
export async function subscribeResults(
  chatId: string,
  onMessage: (msg: any) => void
): Promise<() => Promise<void>> {
  const ch = await getChannel();

  // תור זמני בלעדי למנוי זה
  const q = await ch.assertQueue('', {
    exclusive: true,
    durable: false,
    autoDelete: true,
  });

  await ch.bindQueue(q.queue, RESULTS_EX, chatId);

  let consumerTag: string | undefined;

  const res = await ch.consume(
    q.queue,
    (m: ConsumeMessage | null) => {
      if (!m) return;
      try {
        const payload = JSON.parse(m.content.toString('utf8'));
        onMessage(payload);
      } catch {
        // מתעלמים משגיאת JSON
      }
      ch.ack(m);
    },
    { noAck: false }
  );

  consumerTag = res.consumerTag;

  return async () => {
    try {
      if (consumerTag) await ch.cancel(consumerTag);
    } catch {}
    try {
      await ch.unbindQueue(q.queue, RESULTS_EX, chatId);
    } catch {}
    // התור יימחק אוטומטית (exclusive + autoDelete)
  };
}

/* ---------- Wait-once: המתנה חד-פעמית לתוצאה עבור chatId+jobId ---------- */
export async function waitForResult<T = any>(
  chatId: string,
  jobId: string,
  timeoutMs = 60_000
): Promise<T> {
  const c = await getConnection();
  const ch = (await (c as any).createChannel()) as Channel;
  await ch.assertExchange(RESULTS_EX, 'direct', { durable: true });

  const q = await ch.assertQueue('', {
    exclusive: true,
    durable: false,
    autoDelete: true,
  });
  await ch.bindQueue(q.queue, RESULTS_EX, chatId);

  let consumerTag: string | undefined;

  return new Promise<T>(async (resolve, reject) => {
    let finished = false;

    const cleanup = async () => {
      try { if (consumerTag) await ch.cancel(consumerTag); } catch {}
      try { await ch.unbindQueue(q.queue, RESULTS_EX, chatId); } catch {}
      try { await ch.deleteQueue(q.queue); } catch {}
      try { await ch.close(); } catch {}
    };

    const timer = setTimeout(async () => {
      if (finished) return;
      finished = true;
      await cleanup();
      reject(new Error('result timeout'));
    }, timeoutMs);

    const res = await ch.consume(
      q.queue,
      async (m: ConsumeMessage | null) => {
        if (!m || finished) return;
        try {
          const payload = JSON.parse(m.content.toString('utf8'));
          // אם ה-Worker מצרף jobId — מסננים לפי jobId; אחרת מקבלים את הראשון
          if (!jobId || payload?.jobId === jobId) {
            finished = true;
            ch.ack(m);
            clearTimeout(timer);
            await cleanup();
            resolve(payload as T);
            return;
          }
        } catch {
          // שגיאת JSON → נחזיר לתור לנסיון נוסף
        }
        ch.nack(m, false, true);
      },
      { noAck: false }
    );

    consumerTag = res.consumerTag;
  });
}

/* ---------- ייצוא קונפיג (אם צריך בצד הקורא) ---------- */
export const RabbitConfig = {
  RABBIT_URL,
  JOBS_QUEUE,
  RESULTS_EX,
};
