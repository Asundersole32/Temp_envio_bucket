const express = require('express');
const multer = require('multer');
const { Storage } = require('@google-cloud/storage');
const path = require('path');
const cors = require('cors');
const fs = require('fs').promises;
const os = require('os');
const AdmZip = require('adm-zip');
const { v4: uuidv4 } = require('uuid');

const app = express();
const PORT = 3000;

// Configurações do CORS
app.use(cors({ origin: '*', methods: ['GET', 'POST', 'OPTIONS'] }));

// Configurações do Google Cloud Storage
const bucketName = 'saeb-receive';
const storage = new Storage({
  keyFilename: path.resolve(
    __dirname, 
    'config',
    'credencial_innyx-tecnologia-f7efaf00e6ea.json'
  )
});
const bucket = storage.bucket(bucketName);

// Middleware para upload
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 500 * 1024 * 1024,
    files: 10
  },
  fileFilter: (req, file, cb) => {
    if (path.extname(file.originalname) === '.zip') {
      cb(null, true);
    } else {
      cb(new Error('Apenas arquivos ZIP são permitidos'), false);
    }
  }
});

// Armazenamento de sessões
const sessions = new Map();

// Nova rota SSE para progresso
app.get('/progress/:sessionId', (req, res) => {
  const sessionId = req.params.sessionId;
  
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');
  
  // Enviar eventos existentes
  if (sessions.has(sessionId)) {
    const session = sessions.get(sessionId);
    session.events.forEach(event => {
      res.write(`event: ${event.type}\n`);
      res.write(`data: ${JSON.stringify(event.data)}\n\n`);
    });
  }
  
  // Armazenar a conexão para envio futuro
  sessions.set(sessionId, {
    res,
    events: sessions.get(sessionId)?.events || []
  });
  
  req.on('close', () => {
    sessions.delete(sessionId);
  });
});

// Função para enviar eventos
function sendEvent(sessionId, type, data) {
  if (sessions.has(sessionId)) {
    const event = { type, data };
    const session = sessions.get(sessionId);
    
    // Armazenar evento
    session.events.push(event);
    
    // Enviar imediatamente se a conexão estiver ativa
    if (session.res) {
      session.res.write(`event: ${type}\n`);
      session.res.write(`data: ${JSON.stringify(data)}\n\n`);
    }
  }
}

// Rota de upload
app.post('/upload', 
  upload.array('zipFiles'),
  async (req, res, next) => {
    const sessionId = uuidv4();
    sessions.set(sessionId, { events: [] });
    
    // Responder imediatamente com sessionId
    res.json({ sessionId });
    
    // Declarar tempDir no escopo externo
    let tempDir;
    
    try {
      tempDir = path.join(os.tmpdir(), `upload-${sessionId}`);
      await fs.mkdir(tempDir, { recursive: true });

      const results = {};
      
      // Processar cada arquivo ZIP
      for (const file of req.files) {
        const zipId = file.originalname;
        results[zipId] = { total: 0, uploaded: 0, files: [] };
        
        const zip = new AdmZip(file.buffer);
        const zipEntries = zip.getEntries();
        const totalFiles = zipEntries.filter(entry => !entry.isDirectory).length;
        results[zipId].total = totalFiles;

        // Enviar evento de progresso inicial
        sendEvent(sessionId, 'progress', {
          zipId,
          progress: 0,
          uploaded: 0,
          totalFiles
        });

        let uploadedCount = 0;
        for (const entry of zipEntries) {
          if (!entry.isDirectory) {
            const fileName = path.basename(entry.entryName);
            const tempFilePath = path.join(tempDir, fileName);
            
            // Extrair arquivo
            await new Promise((resolve, reject) => {
              entry.getDataAsync((data, err) => {
                if (err) return reject(err);
                fs.writeFile(tempFilePath, data).then(resolve).catch(reject);
              });
            });

            // Upload para GCS
            const url = await uploadFileToGCS(tempFilePath, fileName);
            uploadedCount++;
            results[zipId].files.push({ name: fileName, url });
            results[zipId].uploaded = uploadedCount;
            
            // Atualizar progresso
            const progress = Math.round((uploadedCount / totalFiles) * 100);
            sendEvent(sessionId, 'progress', {
              zipId,
              progress,
              uploaded: uploadedCount,
              totalFiles
            });
            
            // Remover arquivo temporário
            await fs.unlink(tempFilePath);
          }
        }
      }

      // Enviar evento de conclusão
      sendEvent(sessionId, 'completed', results);
      
    } catch (err) {
      console.error('Erro no processamento:', err);
      sendEvent(sessionId, 'error', { message: err.message });
    } finally {
      // Remover diretório temporário apenas se foi definido
      if (tempDir) {
        try {
          await fs.rm(tempDir, { recursive: true, force: true });
        } catch (cleanupErr) {
          console.error('Erro na limpeza:', cleanupErr);
        }
      }
      // Limpar sessão após 30 segundos
      setTimeout(() => sessions.delete(sessionId), 30000);
    }
  }
);

// Função para upload de arquivo para GCS
async function uploadFileToGCS(filePath, fileName) {
  const blob = bucket.file("Teste/"+fileName);
  await blob.save(await fs.readFile(filePath));
  return `https://storage.googleapis.com/${bucketName}/${fileName}`;
}

// Rota de verificação de saúde
app.get('/health', (req, res) => {
  res.status(200).json({ 
    status: 'OK', 
    message: 'Servidor está funcionando',
    timestamp: new Date().toISOString()
  });
});

// Middleware para tratamento de erros
app.use((err, req, res, next) => {
  console.error('Erro global:', err);
  res.status(500).json({ error: 'Erro interno do servidor' });
});

// Iniciar servidor
app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
  console.log(`Endpoint de upload: http://localhost:${PORT}/upload`);
  console.log(`Endpoint de saúde: http://localhost:${PORT}/health`);
});