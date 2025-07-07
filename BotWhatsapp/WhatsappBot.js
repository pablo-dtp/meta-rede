const WhatsappService = require('./services/WhatsappService');
const logger = require('./logger');

class WhatsappBot {
    constructor() {
        this.whatsappService = WhatsappService;
        this.client = this.whatsappService.getClient();
    }

    iniciar() {
        return new Promise((resolve, reject) => {
            this.client.on('ready', () => {
                logger.info('WhatsApp está pronto.');
                resolve();
            });

            this.client.on('auth_failure', (msg) => {
                logger.error(`Falha na autenticação: ${msg}`);
                reject(msg);
            });

            this.client.on('disconnected', (reason) => {
                logger.warn(`Desconectado: ${reason}`);
            });

            // Já inicializou no WhatsappService constructor
        });
    }

    async listarGrupos() {
        const chats = await this.client.getChats();
        const grupos = chats.filter(chat => chat.isGroup);
        grupos.forEach(grupo => {
            logger.info(`Nome: ${grupo.name} - ID: ${grupo.id._serialized}`);
        });
    }

    async enviarMensagem(numeroOuId, mensagem, caminhoArquivo = null) {
        if (caminhoArquivo) {
            return await this.whatsappService.sendFile(numeroOuId, caminhoArquivo, mensagem);
        } else {
            return await this.whatsappService.sendText(numeroOuId, mensagem);
        }
    }

    async fechar() {
        await this.whatsappService.destroy();
    }
}

module.exports = WhatsappBot;
