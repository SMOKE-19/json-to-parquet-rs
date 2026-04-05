# `json_to_parquet_rs`

`json_to_parquet_rs`는 저장된 JSON 파일을 Rust 구현으로 읽어 typed Parquet 파일로 변환하는 Python 패키지다.

이 디렉터리가 실제 editable 설치 루트다.

## 현재 구조

- `pyproject.toml`: Python 패키지 메타데이터
- `Cargo.toml`: Rust crate 설정
- `src/json_to_parquet_rs/__init__.py`: Python 래퍼
- `src/`: Rust 구현과 Python 패키지 소스
- `test/`: 샘플 입력 파일

## 공개 API

- `convert_json_to_parquet()`
- `convert_json_to_parquet_passthrough()`

두 함수 모두 저장된 JSON 파일 경로를 받아 Parquet 파일을 만든다.

## 특징

- 입력 JSON 전체를 Python 메모리에 적재하지 않는 방향의 래퍼
- Rust 확장 호출
- lookup parquet 기반 변환 지원
- restore 없이 passthrough 변환 지원

## 설치

```bash
pip install -e .
```

자세한 호출 예시는 [USAGE.md](./USAGE.md) 를 보면 된다.
