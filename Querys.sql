

-- SELECT * FROM public.municipios;

-- SELECT * FROM public.operaciones; -- contiene datos

-- SELECT * FROM public.productos; -- contiene datos

-- SELECT * FROM public.tamanio;

-- -- TEMPORAL
-- SELECT * FROM public.temporal; 



-- OPERACIONES
SELECT * FROM public.operaciones; -- contiene datos

SELECT * FROM public.operaciones WHERE id_departamento = 5741; -- tiene 331 registros (departamento huila)
SELECT * FROM public.departamentos where id_departamento = 5741; -- el departamento huila pertenece a la region 3
SELECT * FROM public.regiones WHERE id_region = 3; -- el ID 3 pertenece a la "Región Centro Sur"

UPDATE public.operaciones AS tb_oper
SET id_region = tb_dep.codigo_region
FROM departamentos AS tb_dep
WHERE tb_oper.id_departamento = tb_dep.id_departamento;

