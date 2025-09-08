# Use a imagem oficial do Node.js
FROM node:18-alpine

# Definir diretório de trabalho
WORKDIR /usr/src/app

# Copiar arquivos de configuração de dependências
COPY package*.json ./

# Instalar dependências
RUN npm install --only=production

# Copiar código da aplicação
COPY . .

# Expor a porta
EXPOSE 8080

# Executar a aplicação
CMD [ "node", "server.js" ]