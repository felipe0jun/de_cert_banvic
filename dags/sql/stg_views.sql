CREATE SCHEMA IF NOT EXISTS stg;

DROP VIEW IF EXISTS stg.agencias;
CREATE VIEW stg.agencias AS
SELECT
    cod_agencia::integer            AS cod_agencia,
    trim(nome)                      AS nome,
    trim(endereco)                  AS endereco,
    trim(cidade)                    AS cidade,
    upper(trim(uf))                 AS uf,
    data_abertura::date             AS data_abertura,
    trim(tipo_agencia)              AS tipo_agencia
FROM raw.agencias;

DROP VIEW IF EXISTS stg.clientes;
CREATE VIEW stg.clientes AS
WITH base AS (
    SELECT
        cod_cliente::integer                        AS cod_cliente,
        trim(primeiro_nome)                         AS primeiro_nome,
        trim(ultimo_nome)                           AS ultimo_nome,
        lower(trim(email))                          AS email,
        trim(tipo_cliente)                          AS tipo_cliente,
        data_inclusao::timestamptz                  AS data_inclusao,
        regexp_replace(cpfcnpj, '\D', '', 'g')      AS cpf_digitos,
        cpfcnpj                                     AS cpfcnpj_raw,
        data_nascimento::date                       AS data_nascimento,
        trim(endereco)                              AS endereco,
        regexp_replace(cep, '\D', '', 'g')          AS cep_digitos,
        cep                                         AS cep_raw
    FROM raw.clientes
)
, emails AS (
    SELECT email, count(DISTINCT cpf_digitos) AS cpfs_distintos
    FROM base
    GROUP BY email
)
SELECT
    base.cod_cliente,
    base.primeiro_nome,
    base.ultimo_nome,
    base.email,
    base.tipo_cliente,
    base.data_inclusao,
    (data_inclusao AT TIME ZONE 'America/Sao_Paulo')::timestamp         AS data_inclusao_local,
    CASE WHEN length(cpf_digitos) = 11
         THEN regexp_replace(cpf_digitos, '(\d{3})(\d{3})(\d{3})(\d{2})', '\1.\2.\3-\4')
         ELSE cpfcnpj_raw END                                            AS cpfcnpj,
    cpfcnpj_raw,
    data_nascimento,
    endereco,
    CASE WHEN length(cep_digitos) = 8
         THEN regexp_replace(cep_digitos, '(\d{5})(\d{3})', '\1-\2')
         ELSE cep_raw END                                                AS cep,
    cep_raw,
    (emails.cpfs_distintos > 1)                                          AS flag_email_duplicado,
    (length(cep_digitos) <> 8)                                           AS flag_cep_invalido,
    (data_nascimento > current_date - interval '18 years')               AS flag_menor_de_idade
FROM base
JOIN emails USING (email);

DROP VIEW IF EXISTS stg.colaboradores;
CREATE VIEW stg.colaboradores AS
WITH base AS (
    SELECT
        cod_colaborador::integer                    AS cod_colaborador,
        trim(primeiro_nome)                         AS primeiro_nome,
        trim(ultimo_nome)                           AS ultimo_nome,
        lower(trim(email))                          AS email,
        regexp_replace(cpf, '\D', '', 'g')          AS cpf_digitos,
        cpf                                         AS cpf_raw,
        data_nascimento::date                       AS data_nascimento,
        trim(endereco)                              AS endereco,
        regexp_replace(cep, '\D', '', 'g')          AS cep_digitos,
        cep                                         AS cep_raw
    FROM raw.colaboradores
)
SELECT
    cod_colaborador,
    primeiro_nome,
    ultimo_nome,
    email,
    CASE WHEN length(cpf_digitos) = 11
         THEN regexp_replace(cpf_digitos, '(\d{3})(\d{3})(\d{3})(\d{2})', '\1.\2.\3-\4')
         ELSE cpf_raw END                                                AS cpf,
    cpf_raw,
    data_nascimento,
    endereco,
    CASE WHEN length(cep_digitos) = 8
         THEN regexp_replace(cep_digitos, '(\d{5})(\d{3})', '\1-\2')
         ELSE cep_raw END                                                AS cep,
    cep_raw,
    (length(cep_digitos) <> 8)                                           AS flag_cep_invalido
FROM base;

DROP VIEW IF EXISTS stg.colaborador_agencia;
CREATE VIEW stg.colaborador_agencia AS
SELECT
    cod_colaborador::integer    AS cod_colaborador,
    cod_agencia::integer        AS cod_agencia
FROM raw.colaborador_agencia;

DROP VIEW IF EXISTS stg.contas;
CREATE VIEW stg.contas AS
SELECT
    c.num_conta::integer                                                 AS num_conta,
    c.cod_cliente::integer                                               AS cod_cliente,
    c.cod_agencia::integer                                               AS cod_agencia,
    c.cod_colaborador::integer                                           AS cod_colaborador,
    trim(c.tipo_conta)                                                   AS tipo_conta,
    c.data_abertura::timestamptz                                         AS data_abertura,
    (c.data_abertura::timestamptz AT TIME ZONE 'America/Sao_Paulo')::timestamp
                                                                         AS data_abertura_local,
    round(c.saldo_total::numeric, 2)                                     AS saldo_total,
    round(c.saldo_disponivel::numeric, 2)                                AS saldo_disponivel,
    c.data_ultimo_lancamento::timestamptz                                AS data_ultimo_lancamento,
    (c.data_ultimo_lancamento::timestamptz AT TIME ZONE 'America/Sao_Paulo')::timestamp
                                                                         AS data_ultimo_lancamento_local,
    (c.saldo_disponivel::numeric > c.saldo_total::numeric)               AS flag_disponivel_maior_que_total,
    (cl.cod_cliente IS NULL)                                             AS flag_cliente_inexistente
FROM raw.contas c
LEFT JOIN raw.clientes cl ON cl.cod_cliente::integer = c.cod_cliente::integer;

DROP VIEW IF EXISTS stg.propostas_credito;
CREATE VIEW stg.propostas_credito AS
SELECT
    p.cod_proposta::integer                                              AS cod_proposta,
    p.cod_cliente::integer                                               AS cod_cliente,
    p.cod_colaborador::integer                                           AS cod_colaborador,
    p.data_entrada_proposta::timestamptz                                 AS data_entrada_proposta,
    (p.data_entrada_proposta::timestamptz AT TIME ZONE 'America/Sao_Paulo')::timestamp
                                                                         AS data_entrada_proposta_local,
    round(p.taxa_juros_mensal::numeric, 6)                               AS taxa_juros_mensal,
    round(p.valor_proposta::numeric, 2)                                  AS valor_proposta,
    round(p.valor_financiamento::numeric, 2)                             AS valor_financiamento,
    round(p.valor_entrada::numeric, 2)                                   AS valor_entrada,
    round(p.valor_prestacao::numeric, 2)                                 AS valor_prestacao,
    round(p.valor_proposta::numeric + p.valor_entrada::numeric, 2)       AS valor_total_calculado,
    p.quantidade_parcelas::integer                                       AS quantidade_parcelas,
    p.carencia::integer                                                  AS carencia,
    trim(p.status_proposta)                                              AS status_proposta,
    (cl.cod_cliente IS NULL)                                             AS flag_cliente_inexistente,
    (abs(p.valor_financiamento::numeric - (p.valor_proposta::numeric + p.valor_entrada::numeric)) > 0.01)
                                                                         AS flag_valores_inconsistentes
FROM raw.propostas_credito p
LEFT JOIN raw.clientes cl ON cl.cod_cliente::integer = p.cod_cliente::integer;

DROP VIEW IF EXISTS stg.transacoes;
CREATE VIEW stg.transacoes AS
WITH base AS (
    SELECT
        cod_transacao::integer                                           AS cod_transacao,
        num_conta::integer                                               AS num_conta,
        data_transacao::timestamptz                                      AS data_transacao,
        CASE trim(nome_transacao)
            WHEN 'Estorno de Debito' THEN 'Estorno de Débito'
            ELSE trim(nome_transacao)
        END                                                              AS nome_transacao,
        round(valor_transacao::numeric, 2)                               AS valor_transacao
    FROM raw.transacoes
)
SELECT
    b.cod_transacao,
    b.num_conta,
    b.data_transacao,
    (b.data_transacao AT TIME ZONE 'America/Sao_Paulo')::timestamp       AS data_transacao_local,
    (b.data_transacao AT TIME ZONE 'America/Sao_Paulo')::date            AS data_transacao_dia_local,
    b.nome_transacao,
    CASE WHEN b.valor_transacao >= 0 THEN 'Crédito' ELSE 'Débito' END    AS natureza,
    b.valor_transacao,
    abs(b.valor_transacao)                                               AS valor_absoluto,
    (c.num_conta IS NULL)                                                AS flag_conta_inexistente,
    (b.valor_transacao = 0)                                              AS flag_valor_zero
FROM base b
LEFT JOIN raw.contas c ON c.num_conta::integer = b.num_conta;
