"""
Day 8: RAG 테스트용 가상 문서 데이터셋 (쇼핑몰 CS 규정/FAQ/리뷰)

- id: 고정값. Qdrant point ID를 여기서 만들기 때문에, 같은 id로 다시 적재하면
  중복 생성이 아니라 덮어쓰기(upsert)가 된다.
- category: 나중에 payload 필터 검색(Day 9)에 사용
- source: 규정/FAQ/리뷰 구분
- 일부러 "단어는 다르지만 의미는 같은" 질문으로 검색할 수 있게 문장을 다양하게 썼다.
  (예: "돈 돌려받기" -> 환불 규정 문서가 검색되는지 확인)
"""

SAMPLE_DOCS = [
    # ── 환불 ──
    {"id": "refund-001", "category": "refund", "source": "policy",
     "text": "환불은 상품 수령 후 7일 이내에 신청할 수 있습니다. 단순 변심의 경우 왕복 배송비는 고객이 부담합니다."},
    {"id": "refund-002", "category": "refund", "source": "policy",
     "text": "상품에 하자가 있거나 오배송된 경우에는 수령 후 30일 이내에 무료로 환불해 드립니다. 배송비도 판매자가 부담합니다."},
    {"id": "refund-003", "category": "refund", "source": "policy",
     "text": "포장을 개봉했거나 사용한 흔적이 있는 화장품, 식품은 환불이 불가능합니다. 단, 제품 불량인 경우는 예외입니다."},
    {"id": "refund-004", "category": "refund", "source": "faq",
     "text": "환불 승인 후 카드 결제는 영업일 기준 3~5일, 계좌이체는 1~2일 안에 결제 수단으로 금액이 돌아옵니다."},
    {"id": "refund-005", "category": "refund", "source": "faq",
     "text": "쿠폰이나 포인트를 사용해 구매한 경우 환불 시 쿠폰은 유효기간 내에 복구되고 포인트는 즉시 적립됩니다."},

    # ── 배송 ──
    {"id": "ship-001", "category": "shipping", "source": "policy",
     "text": "평일 오후 2시 이전에 결제를 완료하면 당일 출고되며, 수도권은 다음 날 도착합니다."},
    {"id": "ship-002", "category": "shipping", "source": "policy",
     "text": "제주도와 도서산간 지역은 추가 배송비 3,000원이 부과되며 배송에 1~2일이 더 걸립니다."},
    {"id": "ship-003", "category": "shipping", "source": "faq",
     "text": "배송 조회는 마이페이지의 주문 내역에서 송장번호를 눌러 확인할 수 있습니다. 출고 전에는 송장번호가 표시되지 않습니다."},
    {"id": "ship-004", "category": "shipping", "source": "faq",
     "text": "배송지 변경은 상품 출고 전까지만 가능합니다. 출고 이후에는 택배사에 직접 문의해야 합니다."},
    {"id": "ship-005", "category": "shipping", "source": "faq",
     "text": "택배가 분실되었거나 배송 완료로 표시됐는데 받지 못한 경우, 고객센터로 연락 주시면 택배사 조사 후 재발송 또는 환불 처리합니다."},

    # ── 교환 ──
    {"id": "exch-001", "category": "exchange", "source": "policy",
     "text": "사이즈나 색상 교환은 수령 후 7일 이내 1회에 한해 가능하며, 교환 배송비는 고객이 부담합니다."},
    {"id": "exch-002", "category": "exchange", "source": "faq",
     "text": "교환을 원하는 상품을 먼저 보내주셔야 새 상품이 발송됩니다. 재고가 없으면 환불로 전환됩니다."},

    # ── 결제 ──
    {"id": "pay-001", "category": "payment", "source": "faq",
     "text": "신용카드, 체크카드, 계좌이체, 간편결제(카카오페이, 네이버페이, 토스페이)를 지원합니다."},
    {"id": "pay-002", "category": "payment", "source": "faq",
     "text": "무이자 할부는 5만원 이상 결제 시 2~3개월까지 가능하며, 카드사별 프로모션에 따라 달라질 수 있습니다."},
    {"id": "pay-003", "category": "payment", "source": "faq",
     "text": "결제가 두 번 된 것 같다면 주문 내역을 먼저 확인해 주세요. 중복 결제가 맞으면 영업일 기준 2일 안에 자동 취소됩니다."},

    # ── 회원 ──
    {"id": "member-001", "category": "member", "source": "faq",
     "text": "비밀번호를 잊었다면 로그인 화면의 '비밀번호 찾기'에서 가입한 이메일로 재설정 링크를 받을 수 있습니다."},
    {"id": "member-002", "category": "member", "source": "faq",
     "text": "회원 탈퇴 시 보유한 포인트와 쿠폰은 모두 소멸되며 복구할 수 없습니다. 진행 중인 주문이 있으면 탈퇴가 불가능합니다."},
    {"id": "member-003", "category": "member", "source": "policy",
     "text": "VIP 등급은 최근 6개월 구매 금액이 100만원 이상이면 자동으로 부여되며 무료 배송과 추가 적립 혜택을 받습니다."},

    # ── 리뷰 ──
    {"id": "review-001", "category": "review", "source": "review",
     "text": "배송이 정말 빨라서 주문한 다음 날 바로 도착했어요. 포장도 꼼꼼하게 되어 있어서 만족합니다."},
    {"id": "review-002", "category": "review", "source": "review",
     "text": "사진보다 색이 많이 어두워요. 품질은 괜찮은데 기대했던 색상이 아니라서 교환 신청했습니다."},
    {"id": "review-003", "category": "review", "source": "review",
     "text": "택배 상자가 찌그러진 채로 와서 불안했는데 제품은 멀쩡했어요. 고객센터 응대는 친절했습니다."},
    {"id": "review-004", "category": "review", "source": "review",
     "text": "가격 대비 품질이 아주 좋습니다. 세 번째 재구매인데 항상 만족하고 있어요."},
    {"id": "review-005", "category": "review", "source": "review",
     "text": "환불 문의를 남겼는데 답변이 이틀이나 걸려서 답답했어요. 처리 자체는 빨랐습니다."},
    {"id": "review-006", "category": "review", "source": "review",
     "text": "사이즈가 생각보다 작게 나와요. 한 치수 크게 주문하시길 추천합니다."},
]
