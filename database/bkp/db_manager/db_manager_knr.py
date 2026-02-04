        conn.execute("""
            CREATE TABLE IF NOT EXISTS knr.knrs_fx4pd (
                knr_fx4pd VARCHAR,
                partnumber VARCHAR,
                quantidade FLOAT,
                quantidade_unidade VARCHAR,
                criado_em DATE DEFAULT CURRENT_DATE
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS knr.knrs_comum (
                knr VARCHAR,
                knr_fx4pd VARCHAR,
                cor VARCHAR,
                tmamg VARCHAR,
                cod_pais VARCHAR,
                pais VARCHAR,
                modelo VARCHAR,
                partnumber VARCHAR,
                quantidade FLOAT,
                quantidade_unidade VARCHAR,
                criado_em DATE DEFAULT CURRENT_DATE
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS knr.lt22 (
                num_ot VARCHAR,
                partnumber VARCHAR,
                tp_destino VARCHAR,
                posicao_destino VARCHAR,
                quantidade FLOAT,
                unidade_deposito VARCHAR,
                usuario VARCHAR,
                prateleira VARCHAR,
                data_confirmacao DATE,
                hora_confirmacao TIME,
                criado_em DATE DEFAULT CURRENT_DATE,
                num_ot_usado BOOLEAN DEFAULT FALSE
            );
        """)

    