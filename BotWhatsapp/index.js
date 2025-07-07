const express = require('express');
const WhatsappService = require('./services/WhatsappService');
const logger = require('./logger');

const app = express();
app.use(express.json());

const PORT = 3000;

app.listen(PORT, () => {
    logger.info(`Servidor WhatsApp rodando na porta ${PORT}`);
});
