require('dotenv').config();
const express = require('express');
const path = require('path');
const multer = require('multer');
const { createClient } = require('redis');
const { S3Client, PutObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand, GetCommand } = require('@aws-sdk/lib-dynamodb');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// 1. Initialize AWS Clients (Using native SDK configuration)
const s3Client = new S3Client({ region: process.env.AWS_REGION });
const ddbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const docClient = DynamoDBDocumentClient.from(ddbClient);

// 2. Initialize Redis Client
const redisClient = createClient({ url: process.env.REDIS_URL });
redisClient.on('error', (err) => console.error('❌ Redis Client Error', err));
redisClient.connect().then(() => console.log('🔌 Connected to Redis Queue successfully'));

// 3. Configure Multer (Store files temporarily in memory before streaming to S3)
const storage = multer.memoryStorage();
const upload = multer({ 
    storage: storage,
    limits: { fileSize: 50 * 1024 * 1024 } // Max file size: 50MB
});

// 🚀 Endpoint 1: Accept Upload and Enqueue Job
app.post('/api/v1/process', upload.single('mediaFile'), async (req, res) => {
    try {
        const { taskType } = req.body;
        const file = req.file;

        if (!file) {
            return res.status(400).json({ error: 'No file uploaded.' });
        }
        if (!taskType) {
            return res.status(400).json({ error: 'Missing taskType configuration.' });
        }

        const jobId = `job_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        const fileExtension = path.extname(file.originalname);
        const s3Key = `raw/${jobId}${fileExtension}`;

        console.log(`[${jobId}] Processing request received. Uploadings to S3...`);

        // A. Stream raw file up to Amazon S3
        await s3Client.send(new PutObjectCommand({
            Bucket: process.env.AWS_S3_BUCKET,
            Key: s3Key,
            Body: file.buffer,
            ContentType: file.mimetype
        }));

        // B. Save Initial Job State to DynamoDB
        const jobData = {
            jobId: jobId,
            status: 'pending',
            taskType: taskType,
            rawS3Key: s3Key,
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString()
        };

        await docClient.send(new PutCommand({
            TableName: process.env.DYNAMODB_TABLE,
            Item: jobData
        }));

        // C. Push Payload into Redis Queue (List data structure acting as FIFO queue)
        const queuePayload = {
            jobId: jobId,
            taskType: taskType,
            rawS3Key: s3Key
        };

        await redisClient.lPush('media-processor-jobs', JSON.stringify(queuePayload));
        console.log(`[${jobId}] Enqueued to Redis list successfully.`);

        // Respond back to frontend instantly with 202 Accepted status
        res.status(202).json({
            success: true,
            jobId: jobId,
            status: 'pending'
        });

    } catch (error) {
        console.error('Deployment execution failed:', error);
        res.status(500).json({ error: 'Internal Server Error during ingestion workflow.' });
    }
});

// 🔄 Endpoint 2: Poll Status from DynamoDB
app.get('/api/v1/jobs/:jobId', async (req, res) => {
    try {
        const { jobId } = req.params;

        const result = await docClient.send(new GetCommand({
            TableName: process.env.DYNAMODB_TABLE,
            Key: { jobId: jobId }
        }));

        if (!result.Item) {
            return res.status(404).json({ error: 'Job identifier not found.' });
        }

        const job = result.Item;
        const responseData = { jobId: job.jobId, status: job.status };

        // If the background worker has completed the job, generate a secure S3 download URL
        if (job.status === 'completed' && job.outputS3Key) {
            const command = new GetObjectCommand({
                Bucket: process.env.AWS_S3_BUCKET,
                Key: job.outputS3Key
            });
            // URL expires automatically in 15 minutes (900 seconds)
            responseData.downloadUrl = await getSignedUrl(s3Client, command, { expiresIn: 900 });
        } else if (job.status === 'failed') {
            responseData.errorReason = job.error || 'Unknown execution failure';
        }

        res.json(responseData);

    } catch (error) {
        console.error('Failed to fetch job metadata:', error);
        res.status(500).json({ error: 'Internal Server Errors during polling execution.' });
    }
});

app.listen(PORT, () => {
    console.log(`🚀 Automated Media API Layer running on port ${PORT}`);
});