import re

def extract_info(json_obj, linking_key):
    all_texts = []  # This will store lists of found text and image.actionTarget fields within their respective contexts.
    current_texts = []  # Temporary list to store text and image.actionTarget fields within the current context.

    def search(obj, in_elements_context=False):
        nonlocal current_texts
    
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "mediaComponent" or value == "  helped me get this job":
                    continue  # Skip mediaComponent entirely.
                
                if key == "text" and isinstance(value, str):
                    current_texts.append(value)
                elif key == "actionTarget" and isinstance(value, str):
                    current_texts.append(value)
                elif linking_key is not None and key == linking_key and isinstance(value, str):
                    current_texts.append(value)

                if key == "elements" and isinstance(value, list):
                    # If we're entering the elements context, process each element separately.
                    for element in value:
                        if current_texts:
                            all_texts.append(current_texts)
                            current_texts = []
                        search(element, True)
                    # Flush remaining texts if any after processing the elements.
                    if current_texts:
                        all_texts.append(current_texts)
                        current_texts = []
                else:
                    search(value, in_elements_context)

        elif isinstance(obj, list):
            for item in obj:
                search(item, in_elements_context)

    # Start the search from the root of the JSON structure.
    search(json_obj)
    if current_texts:
        all_texts.append(current_texts)
    return all_texts

def detail_to_dict(lst, keyword):
    result_dict = {}
    current_key = None
    buffer = []
    # Relaxed regex pattern to capture LinkedIn period dates
    pattern = re.compile(r"([\w\s]*\d{4})\s*-\s*([\w\s]*\d{4}|Present)\s*·\s*(\d+\s*yrs?\.?\s*\d*\s*mos?\.?|\d+\s*yrs?\.?|\d+\s*mos?\.?)")
    pattern2 = re.compile(r"Skills:.+")

    for sublist in lst:
        # When we find the keyword we assigned what we iterated before to that ID
        if len(sublist) == 1 and keyword in sublist[0]:
            current_key = sublist[0]
            result_dict[current_key] = buffer.copy()
            buffer = []
        else:
            if len(sublist) >= 2:
                # The easiest one to spot are the dates
                dates_indexes = [i for i, line in enumerate(sublist) if pattern.match(line)]
                # Knowing its index gives us an idea of the content of the experience
                dates_index = dates_indexes[0] if dates_indexes else None
                dates = sublist[dates_index] if dates_index is not None else None

                location = sublist[0] if dates_indexes and dates_indexes[0] == 1 else None

                role_index = len(sublist) - 1
                role = sublist[role_index]

                skills_indexes = [i for i, line in enumerate(sublist) if pattern2.match(line)]
                skills_index = skills_indexes[0] if skills_indexes else None
                skills = sublist[skills_index] if skills_index is not None else None

                description = None
                if dates_index is not None:
                    if dates_index + 1 == role_index or dates_index + 1 == skills_index:
                        description = None
                    else:
                        description = sublist[dates_index + 1]

                entry_dict = {
                    'dates': dates,
                    'location': location,
                    'skills': skills,
                    'description': description,
                    'role': role
                }
                buffer.append(entry_dict)

    return result_dict