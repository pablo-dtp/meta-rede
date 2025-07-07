const path = require('path');
const whatsappService = require('./services/WhatsappService');
const logger = require('./logger');

async function enviarRelatorioMensal() {
    try {
        // Argumentos: [0] = inicio, [1] = fim, [2] = mesRef, [3] = anoRef
        const args = process.argv.slice(2);

        let inicio, fim, mesRef, anoRef, mensagem;

        if (args.length >= 4) {
            inicio = args[0];     // exemplo: "agosto/2024"
            fim = args[1];        // exemplo: "julho/2025"
            mesRef = args[2];     // exemplo: "07"
            anoRef = args[3];     // exemplo: "2025"

            mensagem = `Segue o relatório anual de bonificações referente ao período de ${inicio} até ${fim}.`;
        } else {
            // fallback para situação sem argumentos
            const data = new Date();
            mesRef = (data.getMonth() + 1).toString().padStart(2, '0');
            anoRef = data.getFullYear().toString();
            const mesNome = data.toLocaleString('pt-BR', { month: 'long' });

            mensagem = `Segue o relatório mensal de bonificações referente a ${mesNome} de ${anoRef}.`;
        }

        // monta nome do PDF com base no mês/ano referenciado
        const nomeArquivo = `RelatorioBonificacoesAnual-${mesRef}-${anoRef.slice(2)}.pdf`;
        const caminhoRelatorio = path.resolve(__dirname, '..', 'Relatorio', nomeArquivo);

        const grupoId = '558589578930-1501162626@g.us';
        //const grupoId = '120363420838959584@g.us';
        logger.info(`Enviando relatório para o grupo ${grupoId}`);

        await whatsappService.iniciar();

        const enviado = await whatsappService.sendFileToGroup(grupoId, caminhoRelatorio, mensagem);
        if (!enviado) {
            logger.error('Falha ao enviar o arquivo.');
        }

        await new Promise(resolve => setTimeout(resolve, 2000));
        await whatsappService.fechar();

        logger.info('Relatório enviado com sucesso!');
    } catch (error) {
        logger.error(`Erro ao enviar relatório: ${error.message}`);
    }
}

enviarRelatorioMensal();
