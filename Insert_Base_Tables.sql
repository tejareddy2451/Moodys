insert into methodology.meth
select nextval('meth_id_seq'), a.*
from (select distinct
       c.meth_id,
       trim(Methodology_Name) nm,
       b.meth_typ_cd,
       now() at time zone 'utc',
       CURRENT_USER
from  methodology_stg.stg_meth_doc a
      left join methodology.meth c on a.Merged_to_Methodology_name = c.meth_nm
      join methodology.lkp_meth_typ b on a.Methodology_Type_Code = b.METH_TYPE_TXT
where Methodology_Document_Version_Number = (select max(Methodology_Document_Version_Number)
                                            from methodology_stg.stg_meth_doc b
                                            where trim(b.Methodology_Name) = trim(a.Methodology_Name)
                                            )) a;

insert into methodology.meth_doc
select nextval('meth_doc_id_seq'),
	   meth_id,
	   Publishing_Entity_Code,
	   Language_Name,
	   Methodology_Document_Version_Number::int,
	   Methodology_Document_Type_Code,
	   trim(Methodology_Document_Publication_Name),
	   Methodology_Document_CMS_Document_Identifier,
	   left(Methodology_Document_RMC_Document_Identifier,-2),
       Methodology_Document_Publication_Datetime::timestamp,
	   Methodology_Document_Modification_DateTime::timestamp,
       c.meth_doc_Status_cd,
       now() at time zone 'utc',
       CURRENT_USER      
from methodology_stg.stg_meth_doc a,
     methodology.meth b,
     methodology.lkp_meth_doc_status c
where trim(a.Methodology_Name) = b.meth_nm
  and trim(a.Methodology_Status) = c.meth_doc_Status_desc
  order by meth_id,Methodology_Document_Publication_Datetime; 
  
insert into methodology.rfc_doc
select nextval('rfc_id_seq'),
       trim(Request_for_Comment_Publication_Name),
	   Request_for_Comment_Open_Datetime::timestamp,
	   Request_for_Comment_Close_Datetime::timestamp,
	   Request_for_Comment_Original_Close_Datetime::timestamp,
       Request_for_Comment_CMS_Document_Identifier,
	   Request_for_Comment_RMC_Document_Identifier,
	   b.rfc_status_cd,
	   now() at time zone 'utc',
       CURRENT_USER	   
from methodology_stg.stg_rfc_roc a,
     methodology.lkp_rfc_status b
 where a.Request_for_Comment_Status_Code = b.rfc_status_txt;
 
insert into methodology.roc_doc
select rfc_id,
       Result_of_Consultation_Publication_Name,
	   Result_of_Consultation_CMS_Document_Identifier,
	   Result_of_Consultation_RMC_Document_Identifier,
	   Result_of_Consultation_Publication_Date::timestamp,
	   c.roc_status_cd,
	   now() at time zone 'utc',
       CURRENT_USER
from methodology_stg.stg_rfc_roc a,  
     methodology.rfc_doc b,
     methodology.lkp_roc_status c
where Request_for_Comment_CMS_Document_Identifier = rfc_cms_doc_id	 
  and Request_for_Comment_Publication_Name = rfc_publcn_name
  and Request_for_Comment_Open_Datetime::timestamp = rfc_open_dtm
  and Request_for_Comment_Close_Datetime::timestamp = rfc_close_dtm
  and Request_for_Comment_Original_Close_Datetime::timestamp = rfc_origin_close_dtm
  and a.Result_of_Consultation_Status_Code = c.roc_status_txt;
  
insert into methodology.meth_rfc
select rfc_id,
       meth_doc_id,
       now() at time zone 'utc',
       CURRENT_USER
from methodology_stg.stg_rfc_roc a,
     methodology.rfc_doc b,
     methodology.meth_doc c,
     methodology_stg.stg_meth_doc d
where a.batch = d.batch
  and a.Request_for_Comment_CMS_Document_Identifier = b.rfc_cms_doc_id
  and c.lang_iso_alpha_3_cd = d.Language_Name
  and c.meth_vrsn_num = d.Methodology_Document_Version_Number::int
  and c.meth_doc_typ_cd = d.Methodology_Document_Type_Code
  and c.publ_entity_cd = d.Publishing_Entity_Code;  
 
 
	 
	   
  
