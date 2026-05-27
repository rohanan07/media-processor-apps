require('dotenv').config();
const fs = require('fs').promises;
const path = require('path');
const os = require('os');
const { createClient } = require('redis');
const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const sharp = require('sharp');
const ffmpeg = require('fluent-ffmpeg');

// 1. Initialize AWS and Redis Clients
const s3Client = new S3Client({ region: process.env.AWS_REGION });
const ddbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const docClient = DynamoDBDocumentClient.from(ddbClient);

const redisClient = createClient({ url: process.env.REDIS_URL });

// Helper function to convert readable streams into memory buffers
const streamToBuffer = async (stream) => {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
};

// 🔧 Core Feature 1: Compress Images to optimized .webp
async function handleImageCompression(inputBuffer) {
    console.log("🎨 Compressing image to WebP...");
    return await sharp(inputBuffer)
        .resize({ width: 1200, withoutEnlargement: true }) // Scale down if too massive
        .webp({ quality: 80 }) // 80% quality compression is the sweet spot
        .toBuffer();
}

// 🔧 Core Feature 2: Extract frame from Video using ffmpeg
async function handleVideoThumbnail(inputBuffer, jobId) {
    console.log("🎬 Extracting thumbnail frame from video...");
    const tmpDir = os.tmpdir();
    const inputFilePath = path.join(tmpDir, `${jobId}-input.mp4`);
    const outputFilePath = path.join(tmpDir, `${jobId}-output.jpg`);

    // Write input buffer to a temporary file for ffmpeg to read
    await fs.writeFile(inputFilePath, inputBuffer);

    return new Promise((resolve, reject) => {
        ffmpeg(inputFilePath)
            .screenshots({
                timestamps: ['00:00:02.000'], // Grab a frame at the 2-second mark
                filename: path.basename(outputFilePath),
                folder: tmpDir,
                size: '1280x720'
            })
            .on('end', async () => {
                try {
                    const outputBuffer = await fs.readFile(outputFilePath);
                    // Clean up temporary files asynchronously
                    await Promise.all([fs.unlink(inputFilePath), fs.unlink(outputFilePath)]);
                    resolve(outputBuffer);
                } catch (err) {
                    reject(err);
                }
            })
            .on('error', async (err) => {
                await Promise.all([
                    fs.unlink(inputFilePath).catch(() => {}),
                    fs.unlink(outputFilePath).catch(() => {})
                ]);
                reject(err);
            });
    });
}

// 🔄 Main Work Loop
async function startWorker() {
    try {
        await redisClient.connect();
        console.log('🔌 Worker connected to Redis successfully. Listening for tasks...');

        while (true) {
            // brPop blocks the connection until an item is available in the list
            // Timeout set to 0 means wait indefinitely
            const result = await redisClient.brPop('media-processor-jobs', 0);
            
            // result payload shape: { key: 'media-processor-jobs', element: '{...}' }
            const task = JSON.parse(result.element);
            const { jobId, taskType, rawS3Key } = task;

            console.log(`\n📥 Grabbed [${jobId}] from queue. Type: ${taskType}`);

            try {
                // Update state in DynamoDB to "processing"
                await docClient.send(new UpdateCommand({
                    TableName: process.env.DYNAMODB_TABLE,
                    Key: { jobId },
                    UpdateExpression: "set #status = :s, updatedAt = :u",
                    ExpressionAttributeNames: { "#status": "status" },
                    ExpressionAttributeValues: { ":s": "processing", ":u": new Date().toISOString() }
                }));

                // 1. Download file from S3
                const s3Response = await s3Client.send(new GetObjectCommand({
                    Bucket: process.env.AWS_S3_BUCKET,
                    Key: rawS3Key
                }));
                const inputBuffer = await streamToBuffer(s3Response.Body);

                // 2. Process based on task configuration
                let outputBuffer;
                let outputS3Key;

                if (taskType === 'compress_image') {
                    outputBuffer = await handleImageCompression(inputBuffer);
                    outputS3Key = `processed/${jobId}.webp`;
                } else if (taskType === 'extract_thumbnail') {
                    outputBuffer = await handleVideoThumbnail(inputBuffer, jobId);
                    outputS3Key = `processed/${jobId}.jpg`;
                } else {
                    throw new Error(`Unsupported task type: ${taskType}`);
                }

                // 3. Upload final result back to S3
                await s3Client.send(new PutObjectCommand({
                    Bucket: process.env.AWS_S3_BUCKET,
                    Key: outputS3Key,
                    Body: outputBuffer,
                    ContentType: taskType === 'compress_image' ? 'image/webp' : 'image/jpeg'
                }));

                // 4. Update state in DynamoDB to "completed"
                await docClient.send(new UpdateCommand({
                    TableName: process.env.DYNAMODB_TABLE,
                    Key: { jobId },
                    UpdateExpression: "set #status = :s, outputS3Key = :o, updatedAt = :u",
                    ExpressionAttributeNames: { "#status": "status" },
                    ExpressionAttributeValues: { ":s": "completed", ":o": outputS3Key, ":u": new Date().toISOString() }
                }));

                console.log(`✅ Finished processing job [${jobId}] successfully.`);

            } catch (jobError) {
                console.error(`❌ Failed executing job [${jobId}]:`, jobError);
                
                // Set job state to "failed" so frontend can stop polling gracefully
                await docClient.send(new UpdateCommand({
                    TableName: process.env.DYNAMODB_TABLE,
                    Key: { jobId },
                    UpdateExpression: "set #status = :s, #err = :e, updatedAt = :u",
                    ExpressionAttributeNames: { "#status": "status", "#err": "error" },
                    ExpressionAttributeValues: { ":s": "failed", ":e": jobError.message, ":u": new Date().toISOString() }
                }));
            }
        }
    } catch (criticalError) {
        console.error('Critical runtime error inside execution contexts:', criticalError);
        process.exit(1);
    }
}

startWorker();