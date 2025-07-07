const path = require('path');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const logger = require('../logger');

class WhatsappService {
  constructor() {
    // Caminho absoluto para a pasta de sessão, relativa a este arquivo
    const sessionDir = path.resolve(__dirname, '..', '.wwebjs_auth');

    this.client = new Client({
      authStrategy: new LocalAuth({
        clientId: 'botwhatsapp',
        dataPath: sessionDir
      }),
      puppeteer: {
        headless: true,
        args: ['--no-sandbox']
      }
    });

    this.client.on('qr', qr => {
      logger.info('*** ESCANEIE O QR CODE ABAIXO NO WHATSAPP WEB ***');
      qrcode.generate(qr, { small: true });
    });

    this.client.on('ready', () => {
      logger.info('WhatsApp está pronto.');
    });

    this.client.on('auth_failure', msg => {
      logger.error(`Falha na autenticação: ${msg}`);
    });

    this.client.on('disconnected', reason => {
      logger.warn(`Desconectado: ${reason}`);
    });

    this.client.on('loading_screen', (percent, message) => {
      logger.info(`Loading screen: ${percent}% - ${message}`);
    });
  }

  async iniciar() {
    if (!this.client.initialized) {
      await this.client.initialize();

      // aguarda o evento 'ready' para garantir que o cliente está pronto
      await new Promise((resolve) => {
        this.client.once('ready', () => {
          resolve();
        });
      });
    }
  }

  async listarGrupos() {
    logger.info('Iniciando listagem dos grupos...');
    const chats = await this.client.getChats();
    logger.info(`Chats carregados: ${chats.length}`);

    const grupos = chats.filter(chat => chat.isGroup);
    logger.info(`Grupos encontrados: ${grupos.length}`);

    grupos.forEach(grupo => {
      logger.info(`Grupo: ${grupo.name} - ID: ${grupo.id._serialized}`);
    });

    return grupos;
  }

  async sendToGroup(grupoId, mensagem) {
    try {
      await this.client.sendMessage(grupoId, mensagem);
      logger.info(`Mensagem enviada para o grupo ${grupoId}`);
      return true;
    } catch (error) {
      logger.error(`Erro ao enviar para o grupo ${grupoId}: ${error.message}`);
      return false;
    }
  }

  async sendFileToGroup(grupoId, caminhoArquivo, legenda) {
    try {
      const media = MessageMedia.fromFilePath(caminhoArquivo);
      await this.client.sendMessage(grupoId, media, { caption: legenda });
      logger.info(`Arquivo enviado para o grupo ${grupoId}: ${caminhoArquivo}`);
      return true;
    } catch (error) {
      logger.error(`Erro ao enviar arquivo para o grupo ${grupoId}: ${error.message}`);
      return false;
    }
  }

  async fechar() {
    await this.client.destroy();
    logger.info('Cliente WhatsApp finalizado.');
  }
}

module.exports = new WhatsappService();
