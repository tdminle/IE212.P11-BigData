"""
Module tiền xử lý văn bản tiếng Việt
Bao gồm các hàm để làm sạch, chuẩn hóa và tokenize text
"""

import numpy as np
import pandas as pd
from pyvi.ViTokenizer import ViTokenizer
import regex as re
import os
import matplotlib.pyplot as plt
import seaborn as sns
from emot.emo_unicode import UNICODE_EMOJI, EMOTICONS_EMO
from collections import Counter
import random
import logging

# Cấu hình logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Đường dẫn tới các file tài nguyên
STOPWORDS = './data/vietnamese_stop_word/vietnamese-stopwords.txt'
abb_dict_normal_path = './data/dictionary/abb_dict_normal.xlsx'
abb_dict_special_path = './data/dictionary/abb_dict_special.xlsx'
emoji2word_path = './data/dictionary/emoji2word.xlsx'
character2emoji_path = './data/dictionary/character2emoji.xlsx'

# Load stopwords một lần khi module được import (tối ưu performance)
stopwords = set()
try:
    with open(STOPWORDS, "r", encoding="utf-8") as ins:
        stopwords = set(line.strip('\n') for line in ins if line.strip())
    logger.info(f"Đã load {len(stopwords)} stopwords")
except FileNotFoundError:
    logger.warning(f"Không tìm thấy file stopwords: {STOPWORDS}")
except Exception as e:
    logger.error(f"Lỗi khi load stopwords: {e}")

def filter_stop_words(train_sentences, stop_words):
    """
    Loại bỏ stopwords khỏi câu
    
    Args:
        train_sentences (str): Câu cần xử lý
        stop_words (set): Tập stopwords
    
    Returns:
        str: Câu đã loại bỏ stopwords
    """
    if not train_sentences or not isinstance(train_sentences, str):
        return ""
    
    try:
        new_sent = [word for word in train_sentences.split() if word not in stop_words]
        return ' '.join(new_sent)
    except Exception as e:
        logger.error(f"Lỗi trong filter_stop_words: {e}")
        return train_sentences

def check_repeated_character(text):
    """
    Kiểm tra xem text có ký tự lặp liên tiếp không
    
    Args:
        text (str): Text cần kiểm tra
    
    Returns:
        bool: True nếu có ký tự lặp
    """
    if not text or len(text) < 2:
        return False
    
    try:
        text = re.sub('  +', ' ', text).strip()
        # FIX: Logic cũ chỉ cần 1 ký tự lặp là return True, có thể quá strict
        for i in range(len(text) - 1):
            if text[i] == text[i + 1]:
                return True
        return False
    except Exception as e:
        logger.error(f"Lỗi trong check_repeated_character: {e}")
        return False

def check_space(text):
    """
    Kiểm tra xem text có chứa khoảng trắng không
    
    Args:
        text (str): Text cần kiểm tra
    
    Returns:
        bool: True nếu có khoảng trắng
    """
    if not text:
        return False
    return ' ' in text

def check_special_character_numberic(text):
    """
    Kiểm tra xem text có chứa ký tự đặc biệt hoặc số không
    
    Args:
        text (str): Text cần kiểm tra
    
    Returns:
        bool: True nếu có ký tự không phải chữ cái
    """
    if not text:
        return False
    return any(not c.isalpha() for c in text)

def remove_emoji(text):
    for emot in UNICODE_EMOJI:
        text = str(text).replace(emot, ' ')
    text = re.sub('  +', ' ', text).strip()
    return text

# Remove url
def url(text):
    text = re.sub(r'https?://\S+|www\.\S+', ' ', str(text))
    text = re.sub('  +', ' ', text).strip()
    return text

# remove special character
def special_character(text):
    text = re.sub(r'\d+', lambda m: " ", text)
    # text = re.sub(r'\b(\w+)\s+\1\b',' ', text) #remove duplicate number word
    text = re.sub("[~!@#$%^&*()_+{}“”|:\"<>?`´\-=[\]\;\\\/.,]", " ", text)
    text = re.sub('  +', ' ', text).strip()
    return text

# normalize repeated characters
def repeated_character(text):
    text = re.sub(r'(\w)\1+', r'\1', text)
    text = re.sub('  +', ' ', text).strip()
    return text

def mail(text):
    text = re.sub(r'[^@]+@[^@]+\.[^@]+', ' ', text)
    text = re.sub('  +', ' ', text).strip()
    return text

# remove mention tag and hashtag
def tag(text):
    text = re.sub(r"(?:\@|\#|\://)\S+", " ", text)
    text = re.sub('  +', ' ', text).strip()
    return text

# """Remove all mixed words and numbers"""
def mixed_word_number(text):
    text = ' '.join(s for s in text.split() if not any(c.isdigit() for c in s))
    text = re.sub('  +', ' ', text).strip()
    return text

c2e_path = os.path.join(os.getcwd(), character2emoji_path)
character2emoji = pd.read_excel(c2e_path)  # character to emoji
def convert_character2emoji(text):
    text = str(text)
    for i in range(character2emoji.shape[0]):
        text = text.replace(character2emoji.at[i, 'character'], " " + character2emoji.at[i, 'emoji'] + " ")
    text = re.sub('  +', ' ', text).strip()
    return text

e2w_path = os.path.join(os.getcwd(), emoji2word_path)
emoji2word = pd.read_excel(e2w_path)  # emoji to word

def convert_emoji2word(text):
    for i in range(emoji2word.shape[0]):
        text = text.replace(emoji2word.at[i, 'emoji'], " " + emoji2word.at[i, 'word_vn'] + " ")
    text = re.sub('  +', ' ', text).strip()
    return text

adn_path = os.path.join(os.getcwd(), abb_dict_normal_path)
abb_dict_normal = pd.read_excel(adn_path)


def abbreviation_normal(text):  # len word equal 1
    text = str(text)
    temp = ''
    for word in text.split():
        for i in range(abb_dict_normal.shape[0]):
            if str(abb_dict_normal.at[i, 'abbreviation']) == str(word):
                word = str(abb_dict_normal.at[i, 'meaning'])
        temp = temp + ' ' + word
    text = temp
    text = re.sub('  +', ' ', text).strip()
    return text

ads_path = os.path.join(os.getcwd(), abb_dict_special_path)
abb_dict_special = pd.read_excel(ads_path)


def abbreviation_special(text):  # including special character and number
    text = ' ' + str(text) + ' '
    for i in range(abb_dict_special.shape[0]):
        text = text.replace(' ' + abb_dict_special.at[i, 'abbreviation'] + ' ',
                            ' ' + abb_dict_special.at[i, 'meaning'] + ' ')
    text = re.sub('  +', ' ', text).strip()
    return text

def special_character_1(text):  # remove dot and comma
    text = re.sub("[.,?!]", " ", text)
    text = re.sub('  +', ' ', text).strip()
    return text

def abbreviation_kk(text):
    text = str(text)
    for t in text.split():
        if 'kk' in t:
            text = text.replace(t, ' ha ha ')
        else:
            if 'kaka' in t:
                text = text.replace(t, ' ha ha ')
            else:
                if 'kiki' in t:
                    text = text.replace(t, ' ha ha ')
                else:
                    if 'haha' in t:
                        text = text.replace(t, ' ha ha ')
                    else:
                        if 'hihi' in t:
                            text = text.replace(t, ' ha ha ')
    text = re.sub('  +', ' ', text).strip()
    return text

def remove_quality_product(text):
    """Loại bỏ tiền tố 'Chất lượng sản phẩm:'"""
    if not text:
        return ""
    try:
        return text.replace("Chất lượng sản phẩm:", "").strip()
    except:
        return str(text)

def remove_special_function(text):
    """Loại bỏ tiền tố 'Tính năng nổi bật:'"""
    if not text:
        return ""
    try:
        return text.replace("Tính năng nổi bật:", "").strip()
    except:
        return str(text)

def tokenize(text):
    """
    Tokenize văn bản tiếng Việt sử dụng PyVi
    Args:
        text (str): Text cần tokenize
    Returns:
        str: Text đã được tokenize
    """
    if not text:
        return ""
    try:
        text = str(text)
        text = ViTokenizer.tokenize(text)
        return text
    except Exception as e:
        logger.error(f"Lỗi trong tokenize: {e}")
        return str(text)

def preprocessing(text, lowercased=False):
    """
    Pipeline tiền xử lý hoàn chỉnh cho văn bản tiếng Việt
    
    Args:
        text (str): Text cần xử lý
        lowercased (bool): Có chuyển thành chữ thường không (mặc định: False)
    
    Returns:
        str: Text đã được tiền xử lý hoàn toàn
    
    Processing steps:
        1. Loại bỏ các tiền tố không cần thiết
        2. Loại bỏ stopwords
        3. Chuyển chữ thường
        4. Xử lý emoji và ký tự đặc biệt
        5. Loại bỏ URL, email, tags
        6. Xử lý viết tắt
        7. Tokenize
    """
    # Validate input
    if text is None or (isinstance(text, str) and not text.strip()):
        return ""
    
    try:
        # Đảm bảo text là string
        text = str(text)
        
        # Bước 1: Loại bỏ các tiền tố không cần thiết
        text = remove_quality_product(text)
        text = remove_special_function(text)
        
        # Bước 2: Loại bỏ stopwords
        text = filter_stop_words(text, stopwords)
        
        # Bước 3: Chuyển chữ thường
        text = text.lower() 
        
        # Bước 4: Xử lý character -> emoji conversion
        text = convert_character2emoji(text)
        
        # Bước 5: Loại bỏ URL, email và tags
        text = url(text)
        text = mail(text)
        text = tag(text)
        
        # Bước 6: Loại bỏ từ chứa số
        text = mixed_word_number(text)
        
        # Bước 7: Loại bỏ dấu câu đơn giản (comma, dot, etc)
        text = special_character_1(text)
        
        # Bước 8: Xử lý các viết tắt đặc biệt (kk, haha, etc)
        text = abbreviation_kk(text)
        
        # Bước 9: Thay thế viết tắt đặc biệt
        text = abbreviation_special(text)
        
        # Bước 10: Chuyển character -> emoji một lần nữa
        text = convert_character2emoji(text)
        
        # Bước 11: Loại bỏ emoji
        text = remove_emoji(text)
        
        # Bước 12: Chuẩn hóa ký tự lặp
        text = repeated_character(text)
        
        # Bước 13: Loại bỏ ký tự đặc biệt
        text = special_character(text)
        
        # Bước 14: Thay thế viết tắt thường
        text = abbreviation_normal(text)
        
        # Bước 15: Tokenize
        text = tokenize(text)
        
        # Final cleanup
        text = re.sub('  +', ' ', text).strip()
        
        return text
        
    except Exception as e:
        logger.error(f"Lỗi nghiêm trọng trong preprocessing: {e}")
        # Trả về text gốc nếu có lỗi để không mất dữ liệu
        return str(text) if text else ""
