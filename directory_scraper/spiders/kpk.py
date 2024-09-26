import scrapy

class KPKScraper(scrapy.Spider):
    name = "kpk"

    start_urls = ["https://edirektori.kpk.gov.my/edirektori/index_webbm.php?s_pekerja_nama=&s_bahagian_id=1&s_unit_id=&s_seksyen_id1="]

    bahagian_mapping = {
        "Pejabat YB Menteri (YBM)": {"division": 1, "division_sort_order": 1,},
        "Pejabat YB Timbalan Menteri (YBTM)": {"division": 2, "division_sort_order": 2,},
        "Pejabat Ketua Setiausaha (KSU)": {"division": 3, "division_sort_order": 3,},
        "Pejabat Tim. Ketua Setiausaha (Perladangan dan Komoditi) - TKSU(K)": {"division": 4, "division_sort_order": 4,},
        "Pejabat Tim. Ketua Setiausaha (Perancangan Strategik & Pengurusan)-TKSU(P)": {"division": 5, "division_sort_order": 5,},
        "Unit Komunikasi Korporat (UKK)": {"division": 6, "division_sort_order": 6,},
        "Unit Undang-Undang (UUU)": {"division": 7, "division_sort_order": 7,},
        "Unit Audit Dalam (UAD)": {"division": 8, "division_sort_order": 8,},
        "Unit Integriti (UI)": {"division": 10, "division_sort_order": 9,},
        "Bahagian Kemajuan Industri Sawit dan Sago (BISS)": {"division": 12, "division_sort_order": 10,},
        "Bahagian Pembangunan Industri Getah (GET)": {"division": 14, "division_sort_order": 11,},
        "Bahagian Kemajuan Industri Kayu Kayan, Tembakau dan Kenaf (KTK)": {"division": 13, "division_sort_order": 12,},
        "Bahagian Pembangunan Industri Koko dan Lada (IKL)": {"division": 11, "division_sort_order": 13,},
        "Bahagian Biojisim dan Biobahan Api (BBA)": {"division": 20, "division_sort_order": 14,},
        "Bahagian Perancangan Strategik dan Antarabangsa (PSA)": {"division": 15, "division_sort_order": 15,},
        "Bahagian Pengurusan Sumber Manusia (PSM)": {"division": 17, "division_sort_order": 16,},
        "Bahagian Khidmat Pengurusan dan Pembangunan (BKPP)": {"division": 18, "division_sort_order": 17,},
        "Bahagian Penggalakan Inovasi dan Modal Insan Industri (PIMI)": {"division": 16, "division_sort_order": 18,},
        "Bahagian Akaun (BA)": {"division": 19, "division_sort_order": 19,},
        "Bahagian Pengurusan Maklumat (BPM)": {"division": 21, "division_sort_order": 20,}
    }

    def parse(self, response):
        for bahagian_idx in range(1, 22):
            if bahagian_idx != 9:
                form_data = {
                    "s_pekerja_name": "null",
                    "s_bahagian_id": str(bahagian_idx),
                    "s_unit_id": "null",
                    "s_seksyen_id1": "null",
                }
                url = f"https://edirektori.kpk.gov.my/edirektori/index_webbm.php?s_pekerja_name=&s_bahagian_id={bahagian_idx}&s_unit_id=&s_seksyen_id1="
                yield scrapy.FormRequest(
                    url=url,
                    formdata=form_data,
                    callback=self.parse_form,
                )

    def parse_form(self, response):
        current_unit = None
        current_division = None
        sort_order = 1
        for row in response.css("table[class='GridBlueprint'] > tr"):
            if row.attrib["class"] == "GroupCaptionBlueprint":
                current_division = unit if ((unit := row.css("p ::text").get()) and (unit in self.bahagian_mapping.keys())) else current_division
                current_unit = unit if ((unit := row.css("p ::text").get()) and (unit not in self.bahagian_mapping.keys())) else current_unit
            else:
                data_lst = [txt.strip().replace("\xa0", " ") for txt in row.css("p::text").getall() if txt.strip()]
                if not data_lst:
                    continue
                person_data = {
                    "division": current_division,
                    "division_sort_order": self.bahagian_mapping[current_division]["division_sort_order"],
                    "unit": current_unit,
                    "person_name": data_lst[0],
                    "person_sort_order": sort_order,
                    "person_position": data_lst[1],
                    "person_email": data_lst[2],
                    "person_phone": data_lst[3]
                }
                yield {"data": person_data}