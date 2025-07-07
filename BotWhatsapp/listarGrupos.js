const bot = require('./services/WhatsappService');
const logger = require('./logger');

(async () => {
  try {
    await bot.iniciar();

    console.time('listarGrupos');
    const grupos = await bot.listarGrupos();
    console.timeEnd('listarGrupos');

    logger.info(`Total de grupos: ${grupos.length}`);
    grupos.forEach((grupo, idx) => {
      logger.info(`Grupo ${idx + 1}: ${grupo.name} (ID: ${grupo.id})`);
    });
  } catch (error) {
    logger.error('Erro ao listar grupos:', error);
  } finally {
    await bot.fechar();
  }
})();
