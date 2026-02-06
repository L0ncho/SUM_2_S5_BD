/*===============================================
ALL THE BEST
CASO: CALCULO APORTE LEY SBIF CON LOGICA DE AÑO VENCIDO (-1 AÑO ACTUAL)

DESCR.: PROCESO AUTOMATIZADO PARA CALCULAR APORTES LEGALES SBIF CON MANEJO
DE EXCEPCIONES Y ESTRUCTURAS DE MEMORIA.

NOTA DE EJECUCION: SE HA CONFIGURADO LA VARIABLE BIND CON FECHA 31/12/2026 PARA PROCESAR LOS REGISTROS DEL AÑO EN CURSO
===============================================*/
    
SET DEFINE OFF;
SET SERVEROUTPUT ON;
--DEFINIMOS LA VARIABLE BIND PARA EL PERIODO DE EJECUCION
VARIABLE b_fecha_ejecucion VARCHAR2(10);
EXEC :b_fecha_ejecucion := '31/12/2026'; --EJECUCION DEL AÑO ACTUAL

DECLARE
    --VARRAY PARA TIPOS DE TRANSACCION DE TARJETA
    --SE ALMACENAN LOS NOMBRES DE LOS TIPOS QUE DEBEN SER PROCESADOS
    TYPE t_arr_tipos IS VARRAY(5) OF VARCHAR2(100);
    v_tipos_validos t_arr_tipos := t_arr_tipos('Avance en Efectivo', 'Súper Avance en Efectivo');
    
    --VARIABLES PARA EXTRAER DATOS DEL Varray Y USARLOS EN LOS CURSORES
    v_tipo_1 VARCHAR2(100) := v_tipos_validos(1);
    v_tipo_2 VARCHAR(100) := v_tipos_validos(2);
    v_nombre_limpio VARCHAR2(100):=  'Súper Avance en Efectivo';
    
    --EL SIGUIENTE REGISTRO PL/SQL SE UTILIZARA PARA ACUMULAR LOS TOTALES DEL RESUMEN EN MEMORIA ANTES DE INSERTAR
    --SE UTILIZA UN RECORD PARA AGRUPAR LO DATOS TOTALIZADOS QUE SERAN INSERTADOS POSTERIORMENTE EN LA TABLA RESUMEN
    --ESTO PERMITE GUARDAR AMBOS TOTALES COMO UN SOLO OBJETO EN MEMORIA
    TYPE r_totales_grupo IS RECORD (
        acum_monto NUMBER(12) := 0,
        acum_aporte NUMBER(12) := 0
    );
    v_totales r_totales_grupo;
    
    -- DECLARACION DE VARIABLES ESCALARES
    v_anio_proceso NUMBER(4);
    v_porcentaje    NUMBER(3);
    v_monto_aporte NUMBER(12);
    
    -- CONTADORES PARA VALIDACION DE INTEGRIDAD
    v_filas_procesadas  NUMBER := 0;
    v_filas_esperadas NUMBER := 0;
    
    /*>>>MANEJO DE EXCEPCIONES<<<*/
    e_error_validacion  EXCEPTION; --EXCEPCION DEFINIDA POR ALONSO
    e_valor_excedido EXCEPTION; -- EXCEPCION NO PREDENIFIDA 
    PRAGMA EXCEPTION_INIT(e_valor_excedido, -1438); -- hace referencia al error ORA-01438, el cual ocurre cuando se intenta insertar o actualizar una columna numerica con un valor que excede la precision, como el total de digitos en esa columna
    
    
    -->>>CURSORES EXPLICITOS<<<
    --CURSOR 1 PRINCIPAL
    --AGRUPA POR MES Y TIPO PARA CONTROLAR EL FLUJO PRINCIPAL Y LLENAR LA TABLA RESUYMEN
    CURSOR c_resumen IS
        SELECT
            TO_CHAR(tr.fecha_transaccion, 'MMYYYY') AS mes_anno,
            tp.cod_tptran_tarjeta,
            tp.nombre_tptran_tarjeta AS nombre_tipo
        FROM transaccion_tarjeta_cliente tr
        JOIN tipo_transaccion_tarjeta tp ON tr.cod_tptran_tarjeta = tp.cod_tptran_tarjeta
        WHERE EXTRACT(YEAR FROM tr.fecha_transaccion) = v_anio_proceso
            AND (tp.nombre_tptran_tarjeta = v_tipo_1 OR tp.nombre_tptran_tarjeta LIKE 'S%per Avance en Efectivo')
        GROUP BY TO_CHAR(tr.fecha_transaccion, 'MMYYYY'),
                TRUNC(tr.fecha_transaccion, 'MM'),
                 tp.cod_tptran_tarjeta,
                 tp.nombre_tptran_tarjeta
        ORDER BY TRUNC(tr.fecha_transaccion, 'MM')ASC,
                 nombre_tipo ASC;
    
    v_reg_res c_resumen%ROWTYPE;
    
    
    --CURSOR DETALLE: CURSOR PARAMETRIZADO PARA EFICIENCIA; TRAE EL DETALLE SOLO DEL GRUPO ACTUAL 
    CURSOR c_detalle (p_mes VARCHAR2, p_cod_tipo NUMBER) IS
        SELECT
            c.NUMRUN,
            c.DVRUN,
            tr.NRO_TARJETA,
            tr.NRO_TRANSACCION,
            tr.FECHA_TRANSACCION,
            tr.MONTO_TOTAL_TRANSACCION
        FROM transaccion_tarjeta_cliente tr
        JOIN tarjeta_cliente tc ON tr.nro_tarjeta = tc.nro_tarjeta
        JOIN cliente c ON tc.numrun = c.numrun
        WHERE TO_CHAR(tr.fecha_transaccion, 'MMYYYY') = p_mes
            AND tr.cod_tptran_tarjeta = p_cod_tipo
        ORDER BY tr.fecha_transaccion ASC, c.numrun ASC;
        
    v_reg_det c_detalle%ROWTYPE;
    
BEGIN
    -- >>>INICIO DEL PROCESO<<<
       --INICIALIZACION DE VARIABLES DESDE VARRAY
     v_tipo_1 := v_tipos_validos(1);
    
    -- OBTENEMOS EL AÑO DE LA VARIABLE BIND
    v_anio_proceso := EXTRACT(YEAR FROM TO_DATE(:b_fecha_ejecucion, 'DD/MM/YYYY'));
    DBMS_OUTPUT.PUT_LINE('INICIANDO PROCESO SBIF PARA EL AÑO: ' || v_anio_proceso);
    
    
    --VALIDAR EL TOTAL ESPERADO
    SELECT COUNT(*) INTO v_filas_esperadas
    FROM transaccion_tarjeta_cliente tr
    JOIN tipo_transaccion_tarjeta tp ON tr.cod_tptran_tarjeta = tp.cod_tptran_tarjeta
    WHERE EXTRACT(YEAR FROM tr.fecha_transaccion) = v_anio_proceso
        AND (tp.nombre_tptran_tarjeta = 'Avance en Efectivo' 
           OR tp.nombre_tptran_tarjeta LIKE 'S%per Avance en Efectivo');
        
    --TRUNCAMOS LAS TABLAS CORRESPONDIENTES
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_APORTE_SBIF';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_APORTE_SBIF';
    
    
    -->>>PROCESAMIENTO DE LOS CURSORES<<<
    OPEN c_resumen;
    LOOP
        FETCH c_resumen INTO v_reg_res;
        EXIT WHEN c_resumen%NOTFOUND;
        
        /*LOGICA DE NEGOCIO; LIMPIEZA DE DATOS
            SE DETECTA SI EL NOMBRE VIENE 'SUCIO' (EN EL SCRIPT DE POBLAMIENTO FIGURA ' S uper' Y
            SE CORRIGE USANDO EL VALOR OFICIAL ALMACENADO EL EN VARRAY )*/
        IF v_reg_res.nombre_tipo LIKE 'S%per%' THEN
            v_nombre_limpio := v_tipos_validos(2); 
        ELSE
            v_nombre_limpio := v_reg_res.nombre_tipo;
        END IF;
        
        --REINICIO DE LOS ACUMULADORES DEL REGISTRO RECORD
        v_totales.acum_monto := 0;
        v_totales.acum_aporte := 0;
        
        --PROCESAMIENTO PARA EL CURSOR SECUNDARIO
        OPEN c_detalle(v_reg_res.mes_anno, v_reg_res.cod_tptran_tarjeta);
        LOOP
            FETCH c_detalle INTO v_reg_det;
            EXIT WHEN c_detalle%NOTFOUND;
            
            /*LOGICA DE CALCULO; OBTENCION DEL PORCENTAJE SBIF
                SE BUSCA LA TABLA TRAMO_APORTE_SBIF SEGUN EL MONTO TOTAL*/
            BEGIN
                SELECT PORC_APORTE_SBIF
                INTO v_porcentaje
                FROM tramo_aporte_sbif
                WHERE v_reg_det.monto_total_transaccion BETWEEN TRAMO_INF_AV_SAV AND TRAMO_SUP_AV_SAV;
            EXCEPTION
                --EXCEPCION PREDEFINIDA CONTROLADA
                WHEN NO_DATA_FOUND THEN v_porcentaje := 0;
            END;
            
            /*LOGICA DE CALCULO: MONTO DEL APORTE
                REGLA: COMNTO TOTAL * PORCENTAJE /100.
                SE UTILIZA ROUND PARA REDONDEAR AL ENTERO MAS CECANO SEGUN NORMA*/
            v_monto_aporte := ROUND(v_reg_det.monto_total_transaccion * v_porcentaje / 100 );
            
            --ACUMULAMOS EN EL REGISTRO (MEMORIA)
            v_totales.acum_monto := v_totales.acum_monto + v_reg_det.monto_total_transaccion;
            v_totales.acum_aporte := v_totales.acum_aporte + v_monto_aporte;
            
            --INSERTAMOS DETALLE EN LA TABLA, SE VUELVAN LOS DARTOS ACUMULADOS EN EL RECORD HACIA LA TABLA FINAL
            INSERT INTO detalle_aporte_sbif
            (NUMRUN,DVRUN,NRO_TARJETA,NRO_TRANSACCION,FECHA_TRANSACCION,TIPO_TRANSACCION,MONTO_TRANSACCION,APORTE_SBIF)
            
            VALUES (v_reg_det.numrun,v_reg_det.dvrun,v_reg_det.nro_tarjeta, v_reg_det.nro_transaccion, v_reg_det.fecha_transaccion,
                    v_nombre_limpio,v_reg_det.monto_total_transaccion, v_monto_aporte);
            
            v_filas_procesadas := v_filas_procesadas + 1 ;
            
        END LOOP;
        CLOSE c_detalle;
        
        --INSERTAMOS RESUMEN
        
        INSERT INTO resumen_aporte_sbif (MES_ANNO,TIPO_TRANSACCION,MONTO_TOTAL_TRANSACCIONES,APORTE_TOTAL_ABIF )
        VALUES (v_reg_res.mes_anno, v_nombre_limpio, v_totales.acum_monto, v_totales.acum_aporte);
    
    END LOOP;
    CLOSE c_resumen;
    -->>> CIERRE Y CONTROL TRANSACCIONAL<<<
    --CONFIRMACION CONDICIONAL
    IF v_filas_procesadas = v_filas_esperadas THEN
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('PROCESO FINALIZADO');
        DBMS_OUTPUT.PUT_LINE('FILAS PROCESADAS: ' || v_filas_procesadas);
    --SI NO CALZAN LOS CONTADORES, LANZAMOS EXCEP. DE ALONSO
    ELSE
        RAISE e_error_validacion;
    END IF;

EXCEPTION
    --MANEJO DE EXCEPCIONES
    WHEN e_error_validacion THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR: LA CANTIDAD DE FILAS PROCESADAS NO COINCIDE CON EL TOTAL ESPERADO'); 
    WHEN e_valor_excedido THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR: SE INTENTO INSERTAR UN VALOR NUMERICO QUE EXCEDE LA PRECISION (MAYOR) DE LA COLUMNA (ORA-01438)');
        
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR NO CONTROLADO: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('TRANSACCION REVERTIDA (ROLLBACK)');
END;
/
--CONSUTLAS DE VERIFICACION
--SELECT * FROM RESUMEN_APORTE_SBIF;
--SELECT * FROM DETALLE_APORTE_SBIF;
        
    

    

    
    
    



