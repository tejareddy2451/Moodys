CREATE TABLE methodology.meth (
	meth_id int4 NOT NULL,
	merg_to_meth_id int4 NULL,
	meth_nm varchar(255) NOT NULL,
	meth_typ_cd varchar(25) NOT NULL,
	modified_dtm timestamptz NOT NULL,
	modified_user_id varchar(50) NOT NULL,
	CONSTRAINT pk_meth PRIMARY KEY (meth_id),
	CONSTRAINT merged_into FOREIGN KEY (merg_to_meth_id) REFERENCES methodology.meth(meth_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT meth_typ FOREIGN KEY (meth_typ_cd) REFERENCES methodology.lkp_meth_typ(meth_typ_cd) ON DELETE RESTRICT ON UPDATE RESTRICT
);


CREATE TABLE methodology.meth_doc (
	meth_doc_id int4 NOT NULL,
	meth_id int4 NOT NULL,
	publ_entity_cd varchar(25) NOT NULL,
	lang_iso_alpha_3_cd bpchar(3) NOT NULL,
	meth_vrsn_num int4 NOT NULL,
	meth_doc_typ_cd varchar(25) NOT NULL,
	publ_titl_text varchar(255) NOT NULL,
	cms_doc_id varchar(255) NULL,
	rmc_doc_id varchar(255) NULL,
	publcn_dtm timestamptz NULL,
	meth_doc_modified_dtm date NULL,
	status_cd varchar(25) NOT NULL,
	modified_dtm timestamptz NOT NULL,
	modified_user_id varchar(50) NOT NULL
);

CREATE TABLE methodology.rfc_doc (
	rfc_id int4 NOT NULL,
	rfc_publcn_name varchar(255) NOT NULL,
	rfc_open_dtm date NULL,
	rfc_close_dtm date NULL,
	rfc_origin_close_dtm date NULL,
	rfc_cms_doc_id varchar(255) NULL,
	rfc_rmc_doc_id varchar(255) NULL,
	rfc_status_cd varchar(25) NOT NULL,
	modified_dtm timestamptz NOT NULL,
	modified_user_id varchar(50) NOT NULL,
	CONSTRAINT ak1_rfc_doc UNIQUE (rfc_cms_doc_id),
	CONSTRAINT pk_rfc_doc PRIMARY KEY (rfc_id),
	CONSTRAINT rfc_status FOREIGN KEY (rfc_status_cd) REFERENCES methodology.lkp_rfc_status(rfc_status_cd) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE TABLE methodology.roc_doc (
	rfc_id int4 NOT NULL,
	roc_publcn_nm varchar(255) NULL,
	roc_cms_doc_id varchar(255) NULL,
	roc_rmc_doc_id varchar(255) NULL,
	roc_publcn_dtm date NULL,
	roc_status_cd varchar(25) NOT NULL,
	modified_dtm timestamptz NOT NULL,
	modified_user_id varchar(50) NOT NULL,
	CONSTRAINT ak1_roc_doc UNIQUE (roc_cms_doc_id),
	CONSTRAINT pk_roc_doc PRIMARY KEY (rfc_id),
	CONSTRAINT rfc_roc FOREIGN KEY (rfc_id) REFERENCES methodology.rfc_doc(rfc_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT roc_status FOREIGN KEY (roc_status_cd) REFERENCES methodology.lkp_roc_status(roc_status_cd) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE TABLE methodology.meth_rfc (
	rfc_id int4 NOT NULL,
	meth_doc_id int4 NOT NULL,
	modified_dtm timestamptz NOT NULL,
	modified_user_id varchar(50) NOT NULL,
	CONSTRAINT pk_meth_rfc PRIMARY KEY (rfc_id, meth_doc_id),
	CONSTRAINT meth_doc_rfc FOREIGN KEY (meth_doc_id) REFERENCES methodology.meth_doc(meth_doc_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT rfc_meth_rfc FOREIGN KEY (rfc_id) REFERENCES methodology.rfc_doc(rfc_id) ON DELETE RESTRICT ON UPDATE RESTRICT
);
