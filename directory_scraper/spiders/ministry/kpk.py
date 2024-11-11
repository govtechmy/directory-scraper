import scrapy

class KPKScraper(scrapy.Spider):
    name = "kpk"

    start_urls = ["https://edirektori.kpk.gov.my/edirektori/index_webbm.php?s_pekerja_nama=&s_bahagian_id=1&s_unit_id=&s_seksyen_id1="]

    bahagian_mapping = [
        {"division_name": "Pejabat YB Menteri (YBM)", "division_idx": 1, "division_sort": 1},
        {"division_name": "Pejabat YB Timbalan Menteri (YBTM)", "division_idx": 2, "division_sort": 2},
        {"division_name": "Pejabat Ketua Setiausaha (KSU)", "division_idx": 3, "division_sort": 3},
        {"division_name": "Pejabat Tim. Ketua Setiausaha (Perladangan dan Komoditi) - TKSU(K)", "division_idx": 4, "division_sort": 4},
        {"division_name": "Pejabat Tim. Ketua Setiausaha (Perancangan Strategik & Pengurusan)-TKSU(P)", "division_idx": 5, "division_sort": 5},
        {"division_name": "Unit Komunikasi Korporat (UKK)", "division_idx": 6, "division_sort": 6},
        {"division_name": "Unit Undang-Undang (UUU)", "division_idx": 7, "division_sort": 7},
        {"division_name": "Unit Audit Dalam (UAD)", "division_idx": 8, "division_sort": 8},
        {"division_name": "Unit Integriti (UI)", "division_idx": 10, "division_sort": 9},
        {"division_name": "Bahagian Kemajuan Industri Sawit dan Sago (BISS)", "division_idx": 12, "division_sort": 10},
        {"division_name": "Bahagian Pembangunan Industri Getah (GET)", "division_idx": 14, "division_sort": 11},
        {"division_name": "Bahagian Kemajuan Industri Kayu Kayan, Tembakau dan Kenaf (KTK)", "division_idx": 13, "division_sort": 12},
        {"division_name": "Bahagian Pembangunan Industri Koko dan Lada (IKL)", "division_idx": 11, "division_sort": 13},
        {"division_name": "Bahagian Biojisim dan Biobahan Api (BBA)", "division_idx": 20, "division_sort": 14},
        {"division_name": "Bahagian Perancangan Strategik dan Antarabangsa (PSA)", "division_idx": 15, "division_sort": 15},
        {"division_name": "Bahagian Pengurusan Sumber Manusia (PSM)", "division_idx": 17, "division_sort": 16},
        {"division_name": "Bahagian Khidmat Pengurusan dan Pembangunan (BKPP)", "division_idx": 18, "division_sort": 17},
        {"division_name": "Bahagian Penggalakan Inovasi dan Modal Insan Industri (PIMI)", "division_idx": 16, "division_sort": 18},
        {"division_name": "Bahagian Akaun (BA)", "division_idx": 19, "division_sort": 19},
        {"division_name": "Bahagian Pengurusan Maklumat (BPM)", "division_idx": 21, "division_sort": 20},
    ]

    def parse(self, response):
        division_lst = [row["division_name"] for row in self.bahagian_mapping]
        for row in self.bahagian_mapping:
            division_name = row["division_name"]
            division_idx = row["division_idx"]
            division_sort = row["division_sort"]
            form_data = {
                "s_pekerja_name": "null",
                "s_bahagian_id": str(division_idx),
                "s_unit_id": "null",
                "s_seksyen_id1": "null",
            }
            url = f"https://edirektori.kpk.gov.my/edirektori/index_webbm.php?s_pekerja_name=&s_bahagian_id={division_idx}&s_unit_id=&s_seksyen_id1="
            yield scrapy.FormRequest(
                url=url,
                formdata=form_data,
                callback=self.parse_form,
                meta={
                    "division_sort": division_sort,
                    "division_name": division_name,
                    "division_lst": division_lst,
                }
            )

    def parse_form(self, response):
        current_unit = None
        current_division = response.meta["division_name"]
        division_sort = response.meta["division_sort"]
        division_lst = response.meta["division_lst"]
        sort_order = 1
        for row in response.css("table[class='GridBlueprint'] > tr"):
            if row.attrib["class"] == "GroupCaptionBlueprint":
                current_division = unit if ((unit := row.css("p ::text").get()) and (unit in division_lst)) else current_division
                current_unit = unit if ((unit := row.css("p ::text").get()) and (unit not in division_lst)) else current_unit
            else:
                data_lst = [txt.strip().replace("\xa0", " ") for txt in row.css("p::text").getall() if txt.strip()]
                if not data_lst:
                    continue
                person_data = {
                    "org_sort": 26,
                    "org_id": "KPK",
                    "org_name": "KEMENTERIAN PERLADANGAN DAN KOMODITI",
                    "org_type": "ministry",
                    "division_sort": division_sort,
                    "division_name": current_division,
                    "subdivision_name": current_unit,
                    "position_sort": sort_order,
                    "person_name": data_lst[0],
                    "position_name": data_lst[1],
                    "person_phone": data_lst[3],
                    "person_email": data_lst[2],
                    "person_fax": None,
                    "parent_org_id": None
                }
                sort_order += 1
                yield person_data