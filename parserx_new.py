from requests import get
from time import sleep, time
from json import loads
import data_base


def get_data(times):
    tags = data_base.load_tags()

    count_questions_tag = {}
    count_questions_owner_reputation_300 = {}
    count_answer_tag = {}
    count_upvotes_tag = {}
    count_view_tag = {}
    related_tags = {}

    for i in tags:
        count_questions_tag[i] = 0
        count_questions_owner_reputation_300[i] = 0
        count_answer_tag[i] = 0
        count_upvotes_tag[i] = 0
        count_view_tag[i] = 0
        related_tags[i] = ""
    
    page = 1
    index = 1
    api = "https://api.stackexchange.com/2.2/"
    key = "21IxDPqUnDcVv5yn0xdZ)w(("

    while True:
        sleep(1)
        try:
            questions_data = loads(get(api + f"questions?page={page}&fromdate={times[0]}&todate={times[1]}&order=desc&sort=activity&site=stackoverflow&key={key}").text)
            try:
                print(questions_data["backoff"])
            except:
                pass       
        except:
            print("Error: get_question")
            sleep(180)
        for question in questions_data["items"]:
            start_time = time()
            tags_question = question["tags"]
            for tag in tags_question:
                count_questions_owner_reputation_300[tag] = 0
                if tag not in tags:
                    tags.append(tag)
                try:
                    count_questions_tag[tag] += 1
                except KeyError:
                    count_questions_tag[tag] = 1

                try:
                    count_answer_tag[tag] += question["answer_count"]
                except KeyError:
                    count_answer_tag[tag] = question["answer_count"]
                        
                try:
                    count_upvotes_tag[tag] += question["score"]
                except KeyError:
                    count_upvotes_tag[tag] = question["score"]

                try:
                    count_view_tag[tag] += question["view_count"]
                except KeyError:
                    count_view_tag[tag] = question["view_count"]

                try:
                    if question["owner"]["reputation"] > 300:
                        try:
                            count_questions_owner_reputation_300[tag] += 1
                        except KeyError:
                            count_questions_owner_reputation_300[tag] = 1
                except:
                    pass
                
                if tag not in related_tags.keys():
                    sleep(1)
                    while True:
                        try:
                            tag_related = loads(get(api + f"tags/{tag}/related?site=stackoverflow&key={key}").text)
                        except:
                            print("Error: get_related_tag")
                            sleep(180)
                            continue
                        try:
                            tags_related = ", ".join([tag["name"] for tag in tag_related["items"][1:4]])
                            related_tags[tag] = tags_related
                        except:
                            related_tags[tag] = ""
                        break
                    try:
                        data_base.insert_data([tag, 
                                               count_questions_tag[tag], 
                                                count_answer_tag[tag], 
                                                count_upvotes_tag[tag], 
                                                count_view_tag[tag], 
                                                count_questions_owner_reputation_300[tag], 
                                                related_tags[tag]])
                    except:
                        data_base.insert_data([tag, 
                                              count_questions_tag[tag], 
                                              count_answer_tag[tag], 
                                            count_upvotes_tag[tag], 
                                            count_view_tag[tag], 
                                            0, 
                                            related_tags[tag]])
            end_time = time()
            print(f"Complete question [{index}], time: {end_time - start_time}")
            index += 1

        if questions_data["has_more"] == True:
            page += 1
        else:
            break
    sql_data_save([tags, count_questions_tag, 
                        count_answer_tag, 
                        count_upvotes_tag, 
                        count_view_tag, 
                        count_questions_owner_reputation_300])
    data_base.get_all_data()


def sql_data_save(data):
    data_special = {}
    for tag in data[0]:
        data_special[tag] = [item[tag] for item in data[1:]]
    data_base.update_data(data_special)


get_data([1599004800, 1601510400])