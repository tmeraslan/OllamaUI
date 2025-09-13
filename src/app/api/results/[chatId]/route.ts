// src/app/api/results/[chatId]/route.ts

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

import { subscribeResults } from "@/lib/rabbit";

function renderDetectionMessage(msg: any) {
  const count = msg?.detection_count ?? 0;
  const labels = Array.isArray(msg?.labels) ? msg.labels.join(", ") : "";
  const pid = msg?.prediction_uid ?? "";
  return (
    "Object Detection Results\n" +
    `Detection Count: ${count}\n` +
    `Detected Objects: ${labels}\n` +
    `Prediction ID: ${pid}`
  );
}

export async function GET(
  request: Request,
  { params }: { params: { chatId: string } }
) {
  const chatId = params.chatId || "default-chat";
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    async start(controller) {
      // פונקציית עזר לשליחת אירוע SSE
      const send = (event: string, data: string) => {
        controller.enqueue(encoder.encode(`event: ${event}\n`));
        controller.enqueue(encoder.encode(`data: ${data}\n\n`));
      };

      // keep-alive קל
      const heartbeat = setInterval(() => send("ping", "{}"), 15000);

      // מאזינים ל-results מה-exchange
      const unsubscribe = await subscribeResults(chatId, (msg) => {
        // מייצרים את טקסט התצוגה ומעבירים אותו ישירות ללקוח
        const text = renderDetectionMessage(msg);
        // אפשר לשלוח גם את ה-JSON הגולמי אם תרצה: send("result-json", JSON.stringify(msg));
        send("result-text", JSON.stringify(text));
      });

      const abort = () => {
        clearInterval(heartbeat);
        unsubscribe().catch(() => {});
        controller.close();
      };
      (request as any).signal?.addEventListener?.("abort", abort);

      // מייד מתחילים ב־ready (לא חובה להציג)
      send("ready", JSON.stringify({ chatId }));
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
    },
  });
}
