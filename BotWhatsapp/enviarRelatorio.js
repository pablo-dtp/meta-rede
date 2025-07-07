const path = require('path');
const whatsappService = require('./services/WhatsappService');
const logger = require('./logger');

async function enviarRelatorio() {
    try {
        const data = new Date();
        const ano = data.getFullYear();
        const mesNome = data.toLocaleString('pt-BR', { month: 'long' });
        const mesNumero = data.getMonth() + 1;

        const mensagem = `Segue o relatório de atualização semanal, da Meta da Rede, referente a ${mesNome} de ${ano}.`;

        const nomeArquivo = `RelatorioMetaRede-${mesNumero.toString().padStart(2, '0')}-${ano.toString().slice(2)}.pdf`;
        const caminhoRelatorio = path.resolve(__dirname, '..', 'Relatorio', nomeArquivo);

        const grupoId = '558589578930-1501162626@g.us';
        //const grupoId = '120363420838959584@g.us';
        logger.info(`Enviando mensagem para o grupo ${grupoId}`);

        await whatsappService.iniciar();

        // Envia o arquivo com legenda
        const enviadoArquivo = await whatsappService.sendFileToGroup(grupoId, caminhoRelatorio, mensagem);
        if (!enviadoArquivo) {
            logger.error('Falha ao enviar o arquivo.');
        }

        // Espera 2 segundos para garantir que o arquivo foi enviado antes de fechar
        await new Promise(resolve => setTimeout(resolve, 2000));

        await whatsappService.fechar();

        logger.info('Relatório enviado com sucesso!');
    } catch (error) {
        logger.error(`Erro ao enviar relatório: ${error.message}`);
    }
}

enviarRelatorio();
