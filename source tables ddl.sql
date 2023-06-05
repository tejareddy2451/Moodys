CREATE TABLE methodology_stg.stg_meth_doc (
	batch varchar(200) NULL,
	merged_to_methodology_name varchar(200) NULL,
	methodology_name varchar(200) NULL,
	methodology_type_code varchar(200) NULL,
	methodology_document_cms_document_identifier varchar(200) NULL,
	methodology_document_rmc_document_identifier varchar(200) NULL,
	methodology_document_publication_datetime varchar(200) NULL,
	methodology_document_modification_datetime varchar(200) NULL,
	lob_identifier varchar(200) NULL,
	methodology_document_version_number varchar(200) NULL,
	methodology_document_type_code varchar(200) NULL,
	methodology_document_publication_name varchar(200) NULL,
	publishing_entity_code varchar(200) NULL,
	methodology_status varchar(200) NULL,
	language_name varchar(200) NULL
);


CREATE TABLE methodology_stg.stg_rfc_roc (
	batch varchar(200) NULL,
	request_for_comment_publication_name varchar(200) NULL,
	request_for_comment_open_datetime varchar(200) NULL,
	request_for_comment_close_datetime varchar(200) NULL,
	request_for_comment_original_close_datetime varchar(200) NULL,
	request_for_comment_cms_document_identifier varchar(200) NULL,
	request_for_comment_rmc_document_identifier varchar(200) NULL,
	request_for_comment_publication_datetime varchar(200) NULL,
	request_for_comment_document_modification_datetime varchar(200) NULL,
	request_for_comment_status_code varchar(200) NULL,
	result_of_consultation_publication_name varchar(200) NULL,
	result_of_consultation_cms_document_identifier varchar(200) NULL,
	result_of_consultation_rmc_document_identifier varchar(200) NULL,
	result_of_consultation_publication_date varchar(200) NULL,
	result_of_consultation_status_code varchar(200) NULL
);

CREATE TABLE methodology.lkp_meth_typ (
	meth_typ_cd varchar(25) NOT NULL,
	meth_typ_desc varchar(256) NOT NULL,
	modified_dtm timestamptz NOT NULL,
	modified_user_id varchar(50) NOT NULL,
	CONSTRAINT pk_meth_typ PRIMARY KEY (meth_typ_cd)
);

CREATE TABLE methodology.lkp_meth_doc_status (
	meth_doc_status_cd varchar(25) NOT NULL,
	meth_doc_status_desc varchar(256) NOT NULL,
	modified_dtm timestamptz NOT NULL,
	modified_user_id varchar(50) NOT NULL,
	CONSTRAINT pk_meth_doc_status PRIMARY KEY (meth_doc_status_cd)
);

CREATE TABLE methodology.lkp_rfc_status (
	rfc_status_cd varchar(25) NOT NULL,
	rfc_status_desc varchar(256) NOT NULL,
	modified_dtm timestamptz NOT NULL,
	modified_user_id varchar(50) NOT NULL,
	CONSTRAINT pk_rfc_status PRIMARY KEY (rfc_status_cd)
);

CREATE TABLE methodology.lkp_roc_status (
	roc_status_cd varchar(25) NOT NULL,
	roc_status_desc varchar(256) NOT NULL,
	modified_dtm timestamptz NOT NULL,
	modified_user_id varchar(50) NOT NULL,
	CONSTRAINT pk_roc_status PRIMARY KEY (roc_status_cd)
);

