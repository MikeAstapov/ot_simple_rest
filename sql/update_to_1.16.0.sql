DO
$do$
DECLARE
    gr RECORD;
    c_ord INT;
    dash_gr RECORD;
BEGIN
RAISE NOTICE 'Setting default orders in dashboard groups';
for gr in
    select id, name from "group"
LOOP
    RAISE NOTICE 'Setting dashboard order for group %', gr.name;
    c_ord:=-1;
    for dash_gr in
        select dash_id, "order" from eva.public.dash_group
        where group_id=gr.id
        order by "order"
    LOOP
        c_ord:=c_ord + 1;
        RAISE NOTICE 'Setting dashboard order for dashboard %, old order = %, new order = % ', dash_gr.dash_id, dash_gr.order, c_ord;
        update dash_group set "order" = c_ord
        where group_id=gr.id and dash_id=dash_gr.dash_id;
    end loop;
END LOOP;
END
$do$

