const cors = require('cors');

// Configurar CORS com opções específicas
const corsOptions = {
  origin: '*', // Em produção, substitua pelo domínio do seu frontend
  methods: ['POST', 'GET'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  optionsSuccessStatus: 200
};

app.use(cors(corsOptions));