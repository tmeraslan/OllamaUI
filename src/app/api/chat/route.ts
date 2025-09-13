// src/app/api/chat/route.ts
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { randomUUID } from "crypto";
import { sendJob, waitForResult } from "@/lib/rabbit";

const AWS_REGION = process.env.AWS_REGION || "eu-west-1";
const AWS_S3_BUCKET = process.env.AWS_S3_BUCKET!;
const s3 = new S3Client({ region: AWS_REGION });

const makeKey = () =>
  `randomprefix${
    (globalThis.crypto && typeof globalThis.crypto.randomUUID === "function")
      ? globalThis.crypto.randomUUID()
      : Math.random().toString(36).slice(2)
  }`;

function renderDetectionMessage(result: any) {
  const count = result?.detection_count ?? 0;
  const labels = Array.isArray(result?.labels) ? result.labels.join(", ") : "";
  const pid = result?.prediction_uid ?? "";
  return (
    "Object Detection Results\n" +
    `Detection Count: ${count}\n` +
    `Detected Objects: ${labels}\n` +
    `Prediction ID: ${pid}`
  );
}

export async function POST(req: Request) {
  let outText = "";
  try {
    const { data } = await req.json();
    if (!AWS_S3_BUCKET) throw new Error("Missing AWS_S3_BUCKET");
    if (!data?.images?.length) {
      return new Response(JSON.stringify({ error: "Please provide an image (data.images[0])" }), { status: 400 });
    }

    // 1) העלאה ל-S3
    const imageUrl: string = data.images[0];
    const resp = await fetch(imageUrl);
    if (!resp.ok) throw new Error(`Failed to fetch uploaded image: ${resp.status}`);
    const blob = await resp.blob();
    const arrBuf = await blob.arrayBuffer();
    const s3Key = makeKey();

    await s3.send(
      new PutObjectCommand({
        Bucket: AWS_S3_BUCKET,
        Key: s3Key,
        Body: new Uint8Array(arrBuf),
        ContentType: blob.type || "application/octet-stream",
      })
    );

    // 2) שליחת Job
    const jobId = randomUUID();
    const chatId: string = data?.chatId || "default-chat";
    await sendJob({ jobId, chatId, bucket: AWS_S3_BUCKET, key: s3Key, createdAt: new Date().toISOString() });

    // 3) המתנה לתוצאה (עד 60s) ואז שידור טקסט סופי למשתמש
    const result = await waitForResult<any>(chatId, jobId, 60_000);
    outText = renderDetectionMessage(result);

    const encoder = new TextEncoder();
    const stream = new ReadableStream({
      start(controller) {
        // שורות תוכן בפורמט DataStream ("0:")
        for (const line of outText.split("\n")) {
          controller.enqueue(encoder.encode(`0:${JSON.stringify(line + "\n")}\n`));
        }
        controller.enqueue(
          encoder.encode(
            `e:${JSON.stringify({
              finishReason: "stop",
              usage: { promptTokens: 0, completionTokens: outText.length },
              isContinued: false,
            })}\n`
          )
        );
        controller.enqueue(
          encoder.encode(
            `d:${JSON.stringify({
              finishReason: "stop",
              usage: { promptTokens: 0, completionTokens: outText.length },
            })}\n`
          )
        );
        controller.close();
      },
    });

    return new Response(stream, {
      headers: {
        "Content-Type": "text/plain; charset=utf-8",
        "Cache-Control": "no-cache, no-transform",
        "X-Vercel-AI-Data-Stream": "v1",
        Connection: "keep-alive",
      },
    });
  } catch (err: any) {
    // Fallback אם חל טיימאאוט או שגיאה אחרת
    outText =
      err?.message === "result timeout"
        ? "Processing took longer than expected. Please try again."
        : `❌ Error: ${err?.message || "Unknown error"}`;

    const encoder = new TextEncoder();
    const stream = new ReadableStream({
      start(controller) {
        for (const line of outText.split("\n")) {
          controller.enqueue(encoder.encode(`0:${JSON.stringify(line + "\n")}\n`));
        }
        controller.enqueue(
          encoder.encode(
            `e:${JSON.stringify({
              finishReason: "stop",
              usage: { promptTokens: 0, completionTokens: outText.length },
              isContinued: false,
            })}\n`
          )
        );
        controller.enqueue(
          encoder.encode(
            `d:${JSON.stringify({
              finishReason: "stop",
              usage: { promptTokens: 0, completionTokens: outText.length },
            })}\n`
          )
        );
        controller.close();
      },
    });

    return new Response(stream, {
      status: err?.message === "result timeout" ? 202 : 500,
      headers: {
        "Content-Type": "text/plain; charset=utf-8",
        "Cache-Control": "no-cache, no-transform",
        "X-Vercel-AI-Data-Stream": "v1",
        Connection: "keep-alive",
      },
    });
  }
}







// // app/api/chat/route.ts
// export const runtime = 'nodejs';
// export const dynamic = 'force-dynamic';

// import { S3Client, PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
// import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

// const s3 = new S3Client({ region: 'eu-west-1' });

// const makeKey = () =>
//   `randomprefix${
//     (globalThis.crypto && typeof globalThis.crypto.randomUUID === 'function')
//       ? globalThis.crypto.randomUUID()
//       : Math.random().toString(36).slice(2)
//   }`;

// export async function POST(req: Request) {
//   const { messages, selectedModel, data } = await req.json();
//   let message = 'Please provide an image for object detection.';

//   if (data?.images?.length) {
//     try {
//       const bucket = process.env.AWS_S3_BUCKET!;
//       const yoloService = (process.env.YOLO_SERVICE || 'http://yolo:8081').replace(/\/+$/, '');
//       if (!bucket || !yoloService) throw new Error('Missing AWS_S3_BUCKET or YOLO_SERVICE env');

//       // 1) Upload image to S3
//       const imageUrl: string = data.images[0];
//       const resp = await fetch(imageUrl);
//       if (!resp.ok) throw new Error(`Failed to fetch uploaded image: ${resp.status}`);
//       const blob = await resp.blob();
//       const contentType = blob.type || 'application/octet-stream';
//       const arrBuf = await blob.arrayBuffer();
//       const body = new Uint8Array(arrBuf);
//       const key = makeKey();

//       await s3.send(new PutObjectCommand({
//         Bucket: bucket,
//         Key: key,
//         Body: body,
//         ContentType: contentType,
//       }));

//       // 2) Current logic: YOLO receives only the key, not a signed URL
//       const url = new URL(`${yoloService}/predict`);
//       url.searchParams.set('img_url', key);
//       const predictionResponse = await fetch(url.toString(), { method: 'POST' });

//       if (!predictionResponse.ok) {
//         const text = await predictionResponse.text().catch(() => '');
//         throw new Error(`Prediction API error: ${predictionResponse.status} ${text}`);
//       }

//       const predictionResult = await predictionResponse.json();
//       message =
//         `🔍 **Object Detection Results**
//         **Detection Count:** ${predictionResult.detection_count}
//         **Detected Objects:** ${Array.isArray(predictionResult.labels) ? predictionResult.labels.join(', ') : ''}
//         **Prediction ID:** ${predictionResult.prediction_uid}`;
//     } catch (error) {
//       message =
//       `❌ **Object Detection Error**
//       ${error instanceof Error ? error.message : 'Unknown error'}

//       Tips:
//       - Make sure YOLO has the correct environment variables for S3 (AWS_REGION, AWS_S3_BUCKET, permissions).
//       - Ensure YOLO is listening at: ${process.env.YOLO_SERVICE || 'http://yolo:8081'}.`;
//           }
//         }

//   // === Stream flow in Vercel AI DataStream format ===
//   const encoder = new TextEncoder();
//   const stream = new ReadableStream({
//     start(controller) {
//       // Important: continuous and consistent writing
//       const lines = message.split('\n');
//       for (let i = 0; i < lines.length; i++) {
//         const content = i < lines.length - 1 ? lines[i] + '\n' : lines[i];
//         controller.enqueue(encoder.encode(`0:${JSON.stringify(content)}\n`));
//       }
//       controller.enqueue(encoder.encode(`e:${JSON.stringify({
//         finishReason: 'stop',
//         usage: { promptTokens: 10, completionTokens: message.length },
//         isContinued: false,
//       })}\n`));
//       controller.enqueue(encoder.encode(`d:${JSON.stringify({
//         finishReason: 'stop',
//         usage: { promptTokens: 10, completionTokens: message.length },
//       })}\n`));
//       controller.close();
//     },
//   });

//   return new Response(stream, {
//     headers: {
//       'Content-Type': 'text/plain; charset=utf-8',
//       'Cache-Control': 'no-cache, no-transform',
//       'X-Vercel-AI-Data-Stream': 'v1',
//       Connection: 'keep-alive',
//     },
//   });
// }



