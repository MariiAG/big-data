SELECT id_registro, id_departamento, id_municipio, id_producto, fecha, cantidad, estado
	FROM public.operaciones;
SELECT * from operaciones where cantidad =0;
SELECT * from operaciones where id_departamento =0;
SELECT * from operaciones where cantidad <0;

-- CORRECCION DE FECHAS
SELECT id_registro, fecha FROM operaciones WHERE fecha !~ '^\d{4}-\d{2}-\d{2}$';