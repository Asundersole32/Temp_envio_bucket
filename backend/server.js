const express = require('express');
const { Storage } = require('@google-cloud/storage');
const path = require('path');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const unzipper = require('unzipper');
const { PassThrough } = require('stream');

const app = express();
const PORT = process.env.PORT || 3000;

// ---- Middlewares ----
app.use(cors({ origin: '*', methods: ['GET', 'POST', 'OPTIONS'] }));
app.use(express.json());

// ---- Google Cloud Storage ----
const bucketName = process.env.BUCKET || 'saeb-receive';
const storage = new Storage({
  // Em Cloud Run, prefira ADC (sem key file). Caso esteja rodando localmente, descomente:
  // keyFilename: path.resolve(__dirname, 'config', 'credencial.json')
});
const bucket = storage.bucket(bucketName);

// ---- Sessões e SSE (progresso) ----
const sessions = new Map();

app.get('/progress/:sessionId', (req, res) => {
  const sessionId = req.params.sessionId;
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');

  // Reenvia eventos antigos
  if (sessions.has(sessionId)) {
    const session = sessions.get(sessionId);
    session.events.forEach(event => {
      res.write(`event: ${event.type}\n`);
      res.write(`data: ${JSON.stringify(event.data)}\n\n`);
    });
  }

  // Guarda conexão
  sessions.set(sessionId, {
    res,
    events: sessions.get(sessionId)?.events || []
  });

  req.on('close', () => {
    const s = sessions.get(sessionId);
    if (s) {
      // Não apaga imediatamente para permitir reconexão curta
      s.res = null;
    }
  });
});

function sendEvent(sessionId, type, data) {
  const session = sessions.get(sessionId);
  if (!session) return;
  const event = { type, data };
  session.events.push(event);
  if (session.res) {
    session.res.write(`event: ${type}\n`);
    session.res.write(`data: ${JSON.stringify(data)}\n\n`);
  }
}

// ---- 1) Gera Signed URL (Resumable) ----
// GET /signed-url?fileName=xyz.zip&contentType=application/zip&sessionId=uuid
app.get('/signed-url', async (req, res) => {
  try {
    const { fileName, contentType, sessionId } = req.query;
    if (!fileName || !sessionId) {
      return res.status(400).json({ error: 'fileName e sessionId são obrigatórios' });
    }

    const objectName = `uploads/${sessionId}/${path.basename(fileName)}`;
    const file = bucket.file(objectName);

    const [url] = await file.getSignedUrl({
      version: 'v4',
      action: 'resumable',
      expires: Date.now() + 60 * 60 * 1000, // 15 mins
      contentType: contentType || 'application/octet-stream'
    });

    res.json({ uploadUrl: url, objectName });
  } catch (err) {
    console.error('Erro ao gerar Signed URL:', err);
    res.status(500).json({ error: 'Falha ao gerar Signed URL' });
  }
});

// ---- 2) Processa ZIPs diretamente do GCS (stream) ----
// POST /process  { sessionId, objects: ["uploads/<sessionId>/a.zip", ...] }
app.post('/process', async (req, res) => {
  try {
    const { sessionId, objects } = req.body || {};
    if (!sessionId || !Array.isArray(objects) || objects.length === 0) {
      return res.status(400).json({ error: 'sessionId e objects[] são obrigatórios' });
    }
    
    if (!sessions.has(sessionId)) sessions.set(sessionId, { res: null, events: [] });

    res.json({ ok: true });

    const startedAt = Date.now();
    const zipsCount = objects.length;

    const results = {};
    const summary = {
      zips: zipsCount,
      filesProcessed: 0,
      filesFailed: 0,
      elapsedMs: 0,
      zipStatus: {}
    };

    for (const objectName of objects) {
      const zipId = path.basename(objectName);
      const srcFile = bucket.file(objectName);

      results[zipId] = { total: 0, uploaded: 0, files: [], failed: 0 };
      summary.zipStatus[zipId] = { total: 0, success: 0, failed: 0 };

      sendEvent(sessionId, 'progress', {
        zipId, progress: 0, uploaded: 0, totalFiles: 0
      });

      await new Promise((resolve, reject) => {
        let uploadedCount = 0;
        let failedCount = 0;
        const pendingWrites = [];


        const rs = srcFile.createReadStream().on('error', reject);

        rs.pipe(unzipper.Parse())
          .on('entry', (entry) => {
            if (entry.type === 'Directory') {
              entry.autodrain();
              return;
            }
            
            results[zipId].total += 1;
            summary.zipStatus[zipId].total += 1;

            const fileName = path.basename(entry.path);
            const destPath = `imagens/${fileName}`;
            const passthrough = new PassThrough();
            const ws = bucket.file(destPath).createWriteStream();

            
            const writePromise = new Promise((res, rej) => {
              ws.on('finish', () => {
                uploadedCount += 1;
                results[zipId].uploaded = uploadedCount;
                summary.zipStatus[zipId].success += 1;
                summary.filesProcessed += 1;
                results[zipId].files.push({
                  name: fileName,
                  url: `https://storage.googleapis.com/${bucketName}/${destPath}`,
                  status: 'success'
                });
                const progress = Math.round((uploadedCount / results[zipId].total) * 100);
                sendEvent(sessionId, 'progress', {
                  zipId, progress, uploaded: uploadedCount, totalFiles: results[zipId].total
                });
                res();
              });
              ws.on('error', (err) => {
                failedCount += 1;
                results[zipId].failed = failedCount;
                summary.zipStatus[zipId].failed += 1;
                summary.filesFailed += 1;
                results[zipId].files.push({
                  name: fileName,
                  url: '',
                  status: 'failed',
                  error: err.message
                });
                rej(err);
              });
            });

            pendingWrites.push(writePromise);

            entry.pipe(passthrough).pipe(ws);
          })
          .on('close', () => {
            // Só resolve quando TODAS as escritas terminarem
            Promise.allSettled(pendingWrites).then(() => resolve()).catch(reject);
          })
          .on('error', reject);
      });
    }

    summary.elapsedMs = Date.now() - startedAt;

    // Enviar evento de conclusão com results e summary
    sendEvent(sessionId, 'completed', { results, summary });

    setTimeout(() => sessions.delete(sessionId), 30000000);
  } catch (err) {
    console.error('Erro no /process:', err);
    const { sessionId } = req.body || {};
    if (sessionId) sendEvent(sessionId, 'error', { message: err.message || 'Falha no processamento' });
  }
});

// ---- Health ----
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'OK',
    message: 'Servidor está funcionando',
    timestamp: new Date().toISOString(),
  });
});

// ---- Erros ----
app.use((err, req, res, next) => {
  console.error('Erro global:', err);
  res.status(500).json({ error: 'Erro interno do servidor' });
});

app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
  console.log(`Endpoint de Signed URL: http://localhost:${PORT}/signed-url`);
  console.log(`Endpoint de progresso SSE: http://localhost:${PORT}/progress/:sessionId`);
  console.log(`Endpoint de processar: http://localhost:${PORT}/process`);
  console.log(`Endpoint de saúde: http://localhost:${PORT}/health`);
});
