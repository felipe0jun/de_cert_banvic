CREATE SCHEMA IF NOT EXISTS dq;

CREATE TABLE IF NOT EXISTS dq.check_results (
    run_id       text        NOT NULL,
    check_name   text        NOT NULL,
    severity     text        NOT NULL CHECK (severity IN ('error', 'warn', 'info')),
    failed_rows  bigint      NOT NULL,
    total_rows   bigint      NOT NULL,
    detail       text,
    checked_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, check_name)
);

DELETE FROM dq.check_results WHERE run_id = %(run_id)s;

INSERT INTO dq.check_results (run_id, check_name, severity, failed_rows, total_rows, detail)

SELECT %(run_id)s, 'transacoes_conta_inexistente', 'error',
       count(*) FILTER (WHERE flag_conta_inexistente), count(*),
       'Transações cujo num_conta não existe em contas'
FROM stg.transacoes
UNION ALL
SELECT %(run_id)s, 'transacoes_sinal_inconsistente', 'error',
       count(*) FILTER (WHERE (nome_transacao IN ('Pix - Recebido', 'TED - Recebido', 'DOC - Recebido',
                                                   'Depósito em espécie', 'Estorno de Débito',
                                                   'Transferência entre CC - Crédito') AND valor_transacao < 0)
                           OR (nome_transacao NOT IN ('Pix - Recebido', 'TED - Recebido', 'DOC - Recebido',
                                                       'Depósito em espécie', 'Estorno de Débito',
                                                       'Transferência entre CC - Crédito') AND valor_transacao > 0)),
       count(*),
       'Sinal do valor incompatível com o tipo (entradas devem ser positivas, saídas negativas)'
FROM stg.transacoes
UNION ALL
SELECT %(run_id)s, 'chaves_primarias_nulas', 'error',
       (SELECT count(*) FROM raw.transacoes WHERE cod_transacao IS NULL OR cod_transacao = '')
     + (SELECT count(*) FROM raw.contas WHERE num_conta IS NULL OR num_conta = '')
     + (SELECT count(*) FROM raw.clientes WHERE cod_cliente IS NULL OR cod_cliente = '')
     + (SELECT count(*) FROM raw.propostas_credito WHERE cod_proposta IS NULL OR cod_proposta = ''),
       (SELECT count(*) FROM raw.transacoes) + (SELECT count(*) FROM raw.contas)
     + (SELECT count(*) FROM raw.clientes) + (SELECT count(*) FROM raw.propostas_credito),
       'Chaves primárias nulas ou vazias nas tabelas raw'

UNION ALL
SELECT %(run_id)s, 'contas_cliente_inexistente', 'warn',
       count(*) FILTER (WHERE flag_cliente_inexistente), count(*),
       'Contas cujo cod_cliente não existe em clientes'
FROM stg.contas
UNION ALL
SELECT %(run_id)s, 'propostas_cliente_inexistente', 'warn',
       count(*) FILTER (WHERE flag_cliente_inexistente), count(*),
       'Propostas cujo cod_cliente não existe em clientes'
FROM stg.propostas_credito
UNION ALL
SELECT %(run_id)s, 'clientes_email_duplicado', 'warn',
       count(*) FILTER (WHERE flag_email_duplicado), count(*),
       'Clientes distintos (CPFs diferentes) compartilhando o mesmo e-mail'
FROM stg.clientes
UNION ALL
SELECT %(run_id)s, 'contas_saldo_disponivel_maior_que_total', 'warn',
       count(*) FILTER (WHERE flag_disponivel_maior_que_total), count(*),
       'saldo_disponivel > saldo_total'
FROM stg.contas
UNION ALL
SELECT %(run_id)s, 'transacoes_concentracao_diaria', 'warn',
       CASE WHEN (SELECT max(n) FROM (SELECT count(*) n FROM stg.transacoes GROUP BY data_transacao_dia_local) d) > 0.05 * count(*)
            THEN (SELECT max(n) FROM (SELECT count(*) n FROM stg.transacoes GROUP BY data_transacao_dia_local) d)
            ELSE 0 END,
       count(*),
       'Transações no dia mais movimentado (alerta se passar de 5 por cento do total): pico em 2022-12-29/30 na fonte'
FROM stg.transacoes
UNION ALL
SELECT %(run_id)s, 'clientes_menores_de_idade', 'warn',
       count(*) FILTER (WHERE flag_menor_de_idade), count(*),
       'Clientes com menos de 18 anos'
FROM stg.clientes

UNION ALL
SELECT %(run_id)s, 'clientes_cep_sem_hifen_na_fonte', 'info',
       count(*) FILTER (WHERE cep_raw !~ '^\d{5}-\d{3}$'), count(*),
       'CEPs sem hífen na fonte; normalizados em stg.clientes.cep'
FROM stg.clientes
UNION ALL
SELECT %(run_id)s, 'colaboradores_cep_sem_hifen_na_fonte', 'info',
       count(*) FILTER (WHERE cep_raw !~ '^\d{5}-\d{3}$'), count(*),
       'CEPs sem hífen na fonte; normalizados em stg.colaboradores.cep'
FROM stg.colaboradores
UNION ALL
SELECT %(run_id)s, 'propostas_financiamento_igual_proposta_mais_entrada', 'info',
       count(*) FILTER (WHERE NOT flag_valores_inconsistentes), count(*),
       'valor_financiamento = valor_proposta + valor_entrada (semântica dos nomes invertida na fonte)'
FROM stg.propostas_credito
UNION ALL
SELECT %(run_id)s, 'transacoes_nome_sem_acento_na_fonte', 'info',
       count(*) FILTER (WHERE nome_transacao = 'Estorno de Debito'), count(*),
       '"Estorno de Debito" sem acento na fonte; padronizado em stg'
FROM raw.transacoes
UNION ALL
SELECT %(run_id)s, 'contas_saldo_com_artefato_float', 'info',
       count(*) FILTER (WHERE saldo_total ~ '\.\d{5,}' OR saldo_disponivel ~ '\.\d{5,}'), count(*),
       'Saldos com 5+ casas decimais na fonte; arredondados para 2 em stg'
FROM raw.contas;
