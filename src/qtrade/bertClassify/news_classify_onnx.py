import onnxruntime as ort
from tqdm import tqdm
import torch
import pathlib
from transformers import BertTokenizer

labels = ['finance',
          'realty',
          'stocks',
          'education',
          'science',
          'society',
          'politics',
          'sports',
          'game',
          'entertainment'
          ]
label_dict = dict(zip(range(len(labels)), labels))

DATA_PATH = pathlib.Path(__file__).parent.parent / "data"
MODEL_PATH = DATA_PATH / 'bert_news_classify_fp16_onnxruntime.onnx'
TOKENIZER_PATH = DATA_PATH / "bert-base-chinese"

tokenizer = BertTokenizer.from_pretrained(str(TOKENIZER_PATH))
ort_sess = ort.InferenceSession(str(MODEL_PATH))


def to_onnx(model, seq_len=32, batch_size=32):
    inputs = (torch.randint(0, 21128, size=(batch_size, seq_len)).long(),
              torch.zeros(size=(batch_size, seq_len)).long(),
              torch.ones(size=(batch_size, seq_len)).long())
    input_names = ['input_ids', 'attention_mask', 'token_type_ids']
    output_names = ["classifier"]
    model.eval()
    dynamic_axes = {'input_ids': {0: 'batch_size', 1: 'sequence'},
                    'token_type_ids': {0: 'batch_size', 1: 'sequence'},
                    'attention_mask': {0: 'batch_size', 1: 'sequence'},
                    }
    for i in range(len(output_names)):
        dynamic_axes[f"output_{i}"] = {0: 'batch_size', 1: 'sequence'}
    torch.onnx.export(model,
                      inputs,
                      "bert_new_classify.onnx",
                      export_params=True,
                      verbose=False,
                      opset_version=13,
                      do_constant_folding=True,
                      input_names=input_names,
                      output_names=output_names,  # output names
                      dynamic_axes=dynamic_axes,
                      training=torch.onnx.TrainingMode.EVAL)


def predict_by_onnx(sentences, tokenizer):
    inputs = tokenizer.batch_encode_plus(
        sentences,
        padding="max_length",
        max_length=32,
        truncation="longest_first",
        return_tensors="np")
    outputs = ort_sess.run(None, dict(inputs))
    return outputs[0]


def news2_label(news_string_array):
    batch_size = 32
    out_label_name = []

    for index in tqdm(range(0, len(news_string_array), 32)):
        batch_strings = news_string_array[index:(batch_size + index)]
        out_tensor = predict_by_onnx(batch_strings, tokenizer)
        out_label = out_tensor.argmax(axis=-1)
        out_label_name.extend([label_dict[ele] for ele in out_label.tolist()])
    return out_label_name


if __name__ == "__main__":
    import time
    for i in range(10):
        start_time = time.time()
        out_label_name = news2_label(["a股千股涨停"] * 2)
        print(out_label_name)
        end_time = time.time()
        print(end_time - start_time)
