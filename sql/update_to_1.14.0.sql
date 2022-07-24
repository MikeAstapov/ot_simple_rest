--select * from information_schema.table_constraints;

ALTER TABLE cachesdl DROP CONSTRAINT cachesdl_original_otl_tws_twf_field_extraction_preview_key;

create extension pgcrypto;

create unique index concurrently  cachesdl_original_otl_tws_twf_field_extraction_preview_key ON cachesdl USING btree(
	digest("original_otl", 'sha512'::text),
	tws,
	twf,
	field_extraction,
	preview
);
