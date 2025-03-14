SELECT * FROM public.temporal;

SELECT DISTINCT region from public.temporal;
SELECT DISTINCT codigo_region from public.temporal;

INSERT INTO public.regiones (id_region, nombre)
SELECT DISTINCT codigo_region, region FROM public.temporal;

SELECT * FROM public.regiones;
