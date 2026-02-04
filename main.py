# main.py — Yangilangan FreeFire bot (Captchasiz, yangi tugmalar)
import asyncio
import os
import time
import logging
from typing import Optional

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ContentType
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, BotCommand
)
from dotenv import load_dotenv

from config import OWNER_ID, OWNER2_ID, REQUIRED_CHANNELS_DEFAULT
from database import (
    init_db, add_user, get_user, add_almaz, get_leaderboard,
    get_ref_by, set_ref_by_if_empty, is_verified, set_verified, set_phone_verified,
    list_admins, add_admin, remove_admin, is_admin,
    get_dynamic_text, update_dynamic_text,
    list_required_channels, add_required_channel, remove_required_channel, required_channels_count,
    set_suspension, get_suspension_remaining,
    create_referral, mark_referral_verified, count_verified_referrals, count_all_referrals,
    get_top_referrers_today,
    create_withdraw_request, get_withdraw_request, update_withdraw_status,
    get_withdraw_stats,
    add_withdraw_notification, get_withdraw_notifications,
    backup_database,
    get_setting, set_setting, delete_setting,
    list_offers, get_offer, create_withdraw_and_deduct,
    create_purchase, update_purchase_status, list_pending_purchases,
    create_offer, update_offer, delete_offer, get_user_rank,
    adjust_balance, log_admin_action,
)

PROOF_CHANNEL_SETTING_KEY = "proof_channel_id"
PROOF_CHANNEL_BUTTON = "proof_channel_link"

# ==== Logging ====
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")
log = logging.getLogger("bot")

# ==== ENV / BOT ====
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN .env dan topilmadi")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()


def normalize_proof_channel_value(raw: str) -> str | None:
    if raw is None:
        return None
    value = raw.strip()
    if not value:
        return None

    lowered = value.lower()
    if lowered in {"0", "none", "null", "off"}:
        return None

    if value.startswith(("http://", "https://", "tg://")):
        marker = "t.me/"
        if marker in value:
            after = value.split(marker, 1)[1]
            slug = after.split("?")[0].strip().strip("/")
            slug = slug.lstrip("@")
            if not slug:
                raise ValueError("Kanal username aniqlanmadi.")
            return f"@{slug}"
        raise ValueError("Faqat https://t.me/ ko'rinishidagi havolalar qabul qilinadi.")

    if value.startswith("@"):
        username = value[1:].strip()
        if not username:
            raise ValueError("Username bo'sh bo'lishi mumkin emas.")
        return f"@{username}"

    try:
        int(value)
        return value
    except ValueError:
        raise ValueError("Kanal ID faqat raqamlardan iborat bo'lishi kerak yoki @username ishlating.")


def resolve_proof_chat_id(value: str | None) -> str | int | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    if value.startswith("@"):
        return value
    try:
        return int(value)
    except ValueError:
        return None


def build_proof_channel_url(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    if value.startswith("@"):
        return f"https://t.me/{value[1:]}"
    return None


async def get_proof_channel_value() -> str | None:
    value = await get_setting(PROOF_CHANNEL_SETTING_KEY)
    if not value:
        return None
    value = value.strip()
    return value or None


async def build_proof_button() -> InlineKeyboardButton:
    channel_value = await get_proof_channel_value()
    if not channel_value:
        return InlineKeyboardButton(text="📜 Isbotlar", callback_data=PROOF_CHANNEL_BUTTON)
    url = build_proof_channel_url(channel_value)
    if url:
        return InlineKeyboardButton(text="📜 Isbotlar", url=url)
    return InlineKeyboardButton(text="📜 Isbotlar", callback_data=PROOF_CHANNEL_BUTTON)


async def build_proof_keyboard() -> Optional[InlineKeyboardMarkup]:
    button = await build_proof_button()
    if not button:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[[button]])


# =================== STATES ===================
class VerifyStates(StatesGroup):
    PHONE = State()


class TextEdit(StatesGroup):
    new_text = State()
    section = State()


class Broadcast(StatesGroup):
    WAITING = State()


class ChanManage(StatesGroup):
    ADD = State()
    REMOVE = State()


class AdminManage(StatesGroup):
    ADD = State()
    REMOVE = State()


class OfferManage(StatesGroup):
    ADD = State()
    REMOVE = State()


class SearchUser(StatesGroup):
    WAIT = State()


class WithdrawStates(StatesGroup):
    CHOOSE_GAME = State()
    WAITING_FF_ID = State()


class PurchaseStates(StatesGroup):
    WAITING_PROOF = State()


class WithdrawEdit(StatesGroup):
    WAITING_TEXT = State()


class SuspensionInput(StatesGroup):
    WAIT = State()


class GiveAlmaz(StatesGroup):
    WAIT = State()


class AdjustAchko(StatesGroup):
    ADD = State()
    REMOVE = State()


class ProofChannelSetup(StatesGroup):
    WAITING = State()


class PaymentSetup(StatesGroup):
    CARD = State()
    HOLDER = State()


def format_user_short(name: str, username: Optional[str]) -> str:
    if username:
        return f"@{username}"
    return name


def format_game_label(game: str | None) -> str:
    g = (game or "ff").lower()
    if g == "pubg":
        return "PUBG"
    return "Free Fire"


def parse_user_amount(text: str) -> tuple[int, int] | None:
    raw = (text or "").strip()
    if not raw:
        return None
    if ":" in raw:
        parts = [p.strip() for p in raw.split(":", 1)]
    else:
        parts = raw.split()
    if len(parts) != 2:
        return None
    if not parts[0].isdigit():
        return None
    if not parts[1].lstrip("-").isdigit():
        return None
    return int(parts[0]), int(parts[1])


DEFAULT_CARD_NUMBER = "5614 6816 2639 4070"
DEFAULT_CARD_HOLDER = "Sh.Galya"


async def get_payment_info() -> tuple[str, str]:
    card_number = await get_setting("card_number") or DEFAULT_CARD_NUMBER
    card_holder = await get_setting("card_holder") or DEFAULT_CARD_HOLDER
    return card_number, card_holder


def format_purchase_text(template: str, user_id: int, card_number: str, card_holder: str) -> str:
    return (
        (template or "")
        .replace("{user_id}", str(user_id))
        .replace("{user.id}", str(user_id))
        .replace("{card_number}", card_number)
        .replace("{card_holder}", card_holder)
    )


async def is_owner_or_admin(user_id: int) -> bool:
    # True if user is either owner (OWNER_ID or OWNER2_ID) or an admin
    if user_id == OWNER_ID or user_id == OWNER2_ID:
        return True
    return await is_admin(user_id)


def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID or user_id == OWNER2_ID


async def get_referral_reward() -> int:
    v = await get_setting("referral_reward")
    try:
        return int(v)
    except:
        return 1


# =================== MENUS ===================
main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="💸 Pul ishlash"), KeyboardButton(text="👤 Mening Profilim")],
        [KeyboardButton(text="🏆 Reyting"), KeyboardButton(text="💰 Pul Toldirish")],
        [KeyboardButton(text="🕷️ Free Fire"), KeyboardButton(text="〽️ PUBG")],
        [KeyboardButton(text="📣 Yangiliklar & Bonuslar")],
    ], resize_keyboard=True
)

admin_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Foydalanuvchilar soni")],
        [KeyboardButton(text="📰 Reklama/Yangilik sozlash"), KeyboardButton(text="💰 achko sotib olish matni")],
        [KeyboardButton(text="🟢 achko sotib olish matni")],
        [KeyboardButton(text="💳 Karta raqami"), KeyboardButton(text="👤 Karta egasi")],
        [KeyboardButton(text="📢 Reklama yuborish"), KeyboardButton(text="🔎 Foydalanuvchini topish")],
        [KeyboardButton(text="🧩 Majburiy kanallar"), KeyboardButton(text="🛡 Admin boshqaruvi")],
        [KeyboardButton(text="➕ Pul berish"), KeyboardButton(text="➖ achko olib tashlash")],
        [KeyboardButton(text="📈 Statistika"), KeyboardButton(text="⏳ Tanaffus berish")],
        [KeyboardButton(text="🔧 Referal mukofoti (so'm)"), KeyboardButton(text="📜 Isbotlar kanali")],
        [KeyboardButton(text="💾 Backup yaratish")],
        [KeyboardButton(text="🎯 Withdraw takliflar")],
        [KeyboardButton(text="⬅️ Orqaga")],
    ], resize_keyboard=True
)

back_kb = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="⬅️ Orqaga")]],
    resize_keyboard=True
)


def sub_required_markup(channels: list[str]):
    buttons = [[InlineKeyboardButton(text=f"📢 {ch}", url=f"https://t.me/{ch.lstrip('@')}")] for ch in channels]
    buttons.append([InlineKeyboardButton(text="✅ Tekshirish", callback_data="check_subs")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ================ HELPERS ================
async def setup_bot_commands():
    await bot.set_my_commands([
        BotCommand(command="start", description="Botni ishga tushirish"),
        BotCommand(command="help", description="Yordam"),
        BotCommand(command="admin", description="Admin panel"),
    ])


async def _get_required_channels():
    current = await list_required_channels()
    if not current:
        for ch in REQUIRED_CHANNELS_DEFAULT:
            try:
                await add_required_channel(ch)
            except Exception:
                pass
        current = await list_required_channels()
    return current


async def check_subscription(user_id: int) -> list[str]:
    not_sub = []
    channels = await _get_required_channels()
    for ch in channels:
        chat_ref: Optional[str | int] = ch.strip()
        try:
            if chat_ref.startswith("@"):
                chat_ref = chat_ref
            else:
                try:
                    chat_ref = int(chat_ref)
                except ValueError:
                    chat_ref = chat_ref
            member = await bot.get_chat_member(chat_ref, user_id)
            if member.status not in ("member", "administrator", "creator"):
                not_sub.append(ch)
        except Exception:
            not_sub.append(ch)
    return not_sub


async def guard_common(message: Message) -> bool:
    if message.chat.type != "private":
        return False

    remain = await get_suspension_remaining(message.from_user.id)
    if remain > 0:
        await message.answer(
            "😴 Siz hozircha tanaffusdasiz.\n"
            f"⏰ {remain} soniyadan so'ng bot qayta faollashadi.",
            parse_mode="HTML"
        )
        return True

    not_sub = await check_subscription(message.from_user.id)
    if not_sub:
        await message.answer(
            "🔒 <b>Obuna talab qilinadi</b>\n\n"
            "Quyidagi kanallarimizga a'zo bo'ling, so'ng <b>✅ Tekshirish</b> tugmasini bosing.",
            parse_mode="HTML", reply_markup=sub_required_markup(not_sub)
        )
        return True
    return False


async def set_menu_state(state: FSMContext | None, current: str, prev: str | None):
    if state is None:
        return
    await state.update_data(menu_current=current, menu_prev=prev)


async def render_menu(menu_id: str | None, message: Message, state: FSMContext):
    if menu_id == "admin":
        if not await is_owner_or_admin(message.from_user.id):
            return await message.answer("🚫 Siz admin emassiz.")
        await set_menu_state(state, "admin", "main")
        return await message.answer("👑 <b>Admin panel</b>", parse_mode="HTML", reply_markup=admin_menu)
    if menu_id == "profile":
        return await show_profile(message, state)
    if menu_id == "referral":
        return await earn_almaz(message, state)
    if menu_id == "rating":
        return await show_leaderboard_handler(message, state)
    if menu_id == "shop":
        return await buy_almaz(message, state)

    await set_menu_state(state, "main", None)
    return await message.answer("🏠 Asosiy menyu", reply_markup=main_menu)


async def go_back_menu(message: Message, state: FSMContext):
    data = await state.get_data()
    prev = data.get("menu_prev") or "main"
    return await render_menu(prev, message, state)


async def open_withdraw_menu(message: Message, state: FSMContext, prev_menu: str):
    user_id = message.from_user.id
    user = await get_user(user_id)
    if not user:
        return await message.answer("Avval ro'yxatdan o'ting.")
    balance = user[3] if len(user) > 3 and user[3] is not None else 0

    await state.set_state(WithdrawStates.CHOOSE_GAME)
    await state.update_data(withdraw_prev_menu=prev_menu)
    await set_menu_state(state, "withdraw", prev_menu)

    try:
        ff_offers = await list_offers("ff")
    except Exception:
        ff_offers = []
    try:
        pubg_offers = await list_offers("pubg")
    except Exception:
        pubg_offers = []
    if balance <= 0:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ Orqaga", callback_data="wd_back_prev")]]
        )
        return await message.answer(
            "⚠️ Hali yechib olish uchun balansingiz yetarli emas.",
            reply_markup=kb
        )
    if not ff_offers and not pubg_offers:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ Orqaga", callback_data="wd_back_prev")]]
        )
        return await message.answer(
            "⚠️ Hozircha yechib olish takliflari yo'q.",
            reply_markup=kb
        )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔥 Free Fire", callback_data="wd_game:ff")],
            [InlineKeyboardButton(text="🎯 PUBG", callback_data="wd_game:pubg")],
            [InlineKeyboardButton(text="⬅️ Orqaga", callback_data="wd_back_prev")],
        ]
    )

    await message.answer(
        "💳 <b>achkoni yechib olish</b>\n\n"
        f"Balansingiz: <b>{balance} so'm</b>\n"
        "Iltimos, o'yin bo'limini tanlang:",
        parse_mode="HTML",
        reply_markup=kb
    )


async def send_game_offers(message: Message, state: FSMContext, game: str, prev_menu: str):
    await state.set_state(WithdrawStates.CHOOSE_GAME)
    await state.update_data(withdraw_prev_menu=prev_menu)
    await set_menu_state(state, "withdraw", prev_menu)
    try:
        offers = await list_offers(game)
    except Exception:
        offers = []

    if not offers:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ Orqaga", callback_data="wd_back_prev")]]
        )
        return await message.answer("Hozircha takliflar yo'q.", reply_markup=kb)

    buttons = [[InlineKeyboardButton(text=f"{label} — {cost} som", callback_data=f"wd_offer:{oid}")] for oid, label, cost in offers]
    buttons.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="wd_back_prev")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.answer("Iltimos, yechib olmoqchi bo'lgan taklifni tanlang:", reply_markup=kb)


async def update_withdraw_admin_messages(request_id: int, status_label: str):
    notifications = await get_withdraw_notifications(request_id)
    req = await get_withdraw_request(request_id)
    if not req:
        return
    r_id, user_id, amount, ff_id, game, status, created_at, processed_at, processed_by, note = req
    user = await get_user(user_id)
    username = user[1] if user else None
    almaz = user[3] if user and len(user) > 3 and user[3] is not None else 0
    refs = await count_verified_referrals(user_id)
    game_label = format_game_label(game)
    id_label = "Free Fire ID" if (game or "ff") == "ff" else "PUBG ID"

    created_str = time.strftime("%Y-%m-%d %H:%M", time.localtime(created_at or int(time.time())))
    processed_str = time.strftime("%Y-%m-%d %H:%M", time.localtime(processed_at)) if processed_at else "—"
    note_part = f"\n📝 Izoh: {note}" if note else ""

    base_text = (
        "🧾 <b>Pul yechish so'rovi</b>\n\n"
        f"👤 Foydalanuvchi: @{username or 'Anonim'} (ID: <code>{user_id}</code>)\n"
        f"💎 Joriy balans: <b>{almaz} som</b>\n"
        f"🔥 Yechmoqchi bo'lgan miqdor: <b>{amount} som</b>\n"
        f"👥 Umumiy tasdiqlangan takliflar: <b>{refs}</b>\n"
        f"🎮 O'yin: <b>{game_label}</b>\n"
        f"🎮 {id_label}: <code>{ff_id}</code>\n"
        f"🕒 So'rov vaqti: {created_str}\n"
        f"🕒 Qayta ishlangan: {processed_str}"
        f"{note_part}\n\n"
        f"{status_label}"
    )

    for chat_id, message_id in notifications:
        try:
            await bot.edit_message_text(
                base_text,
                chat_id=chat_id,
                message_id=message_id,
                parse_mode="HTML"
            )
            await bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=None)
        except Exception as e:
            log.warning("withdraw msg edit failed: %s", e)


async def send_proof_receipt(request_id: int, user_id: int, amount: int, ff_id: Optional[str], game: str | None):
    channel_value = await get_proof_channel_value()
    chat_id = resolve_proof_chat_id(channel_value)
    if not chat_id:
        return

    user = await get_user(user_id)
    username = user[1] if user else None
    mention = f"@{username}" if username else f"ID: {user_id}"
    verified_refs = await count_verified_referrals(user_id)
    ff_value = ff_id or "Ko'rsatilmagan"
    now = time.strftime("%d.%m.%Y %H:%M:%S", time.localtime())
    game_label = format_game_label(game)
    id_label = "Free Fire ID" if (game or "ff") == "ff" else "PUBG ID"

    text = (
        "✅ <b>Pul yechish tasdiqlandi</b>\n\n"
        f"👤 Foydalanuvchi: {mention} (ID: <code>{user_id}</code>)\n"
        f"🎮 O'yin: <b>{game_label}</b>\n"
        f"🎮 {id_label}: <code>{ff_value}</code>\n"
        f"💎 Miqdor: <b>{amount} so'm</b>\n"
        f"👥 Umumiy tasdiqlangan takliflari: <b>{verified_refs}</b>\n"
        f"🆔 So'rov ID: <code>{request_id}</code>\n"
        f"🕒 Tasdiqlangan vaqt: {now}"
    )

    try:
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
    except Exception as exc:
        log.warning("Isbotlar kanaliga yuborib bo'lmadi (%s): %s", channel_value, exc)


async def send_withdraw_request_to_proof_channel(request_id: int):
    channel_value = await get_proof_channel_value()
    chat_id = resolve_proof_chat_id(channel_value)
    if not chat_id:
        return

    req = await get_withdraw_request(request_id)
    if not req:
        return

    _, user_id, amount, ff_id, game, status, created_at, _, _, _ = req
    user = await get_user(user_id)
    username = user[1] if user else None
    mention = f"@{username}" if username else f"ID: {user_id}"
    game_label = format_game_label(game)
    id_label = "Free Fire ID" if (game or "ff") == "ff" else "PUBG ID"
    created_str = time.strftime("%Y-%m-%d %H:%M", time.localtime(created_at or int(time.time())))

    text = (
        "🧾 <b>Yangi yechib olish so'rovi</b>\n\n"
        f"👤 Foydalanuvchi: {mention} (ID: <code>{user_id}</code>)\n"
        f"🎮 O'yin: <b>{game_label}</b>\n"
        f"🎮 {id_label}: <code>{ff_id}</code>\n"
        f"💎 Miqdor: <b>{amount} so'm</b>\n"
        f"🕒 So'rov vaqti: {created_str}\n"
        "⏳ Holat: <b>Kutilmoqda</b>"
    )

    try:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as exc:
        log.warning("Isbotlar kanaliga yuborib bo'lmadi (%s): %s", channel_value, exc)


async def notify_admins_about_withdraw(request_id: int):
    req = await get_withdraw_request(request_id)
    if not req:
        return
    r_id, user_id, amount, ff_id, game, status, created_at, processed_at, processed_by, note = req
    user = await get_user(user_id)
    username = user[1] if user else None
    almaz = user[3] if user and len(user) > 3 and user[3] is not None else 0
    refs = await count_verified_referrals(user_id)
    created_str = time.strftime("%Y-%m-%d %H:%M", time.localtime(created_at or int(time.time())))
    game_label = format_game_label(game)
    id_label = "Free Fire ID" if (game or "ff") == "ff" else "PUBG ID"

    text = (
        "🧾 <b>Yangi Pul yechish so'rovi</b>\n\n"
        f"👤 Foydalanuvchi: @{username or 'Anonim'} (ID: <code>{user_id}</code>)\n"
        f"💎 Joriy balans: <b>{almaz} so'm</b>\n"
        f"🔥 Yechmoqchi: <b>{amount} so'm</b>\n"
        f"👥 Umumiy tasdiqlangan takliflar: <b>{refs}</b>\n"
        f"🎮 O'yin: <b>{game_label}</b>\n"
        f"🎮 {id_label}: <code>{ff_id}</code>\n"
        f"🕒 So'rov vaqti: {created_str}\n\n"
        "Quyidagi tugmalar orqali so'rovni boshqaring 👇"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Tasdiqlash", callback_data=f"wd_ok:{request_id}"),
                InlineKeyboardButton(text="✏️ Tahrirlash", callback_data=f"wd_edit:{request_id}"),
                InlineKeyboardButton(text="❌ Rad etish", callback_data=f"wd_reject:{request_id}"),
            ]
        ]
    )

    admin_ids = [OWNER_ID, OWNER2_ID]
    extra_admins = await list_admins()
    for uid, _ in extra_admins:
        if uid not in admin_ids:
            admin_ids.append(uid)

    for chat_id in admin_ids:
        try:
            msg = await bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb)
            await add_withdraw_notification(request_id, chat_id, msg.message_id)
        except Exception as e:
            log.warning("failed to send withdraw notify to %s: %s", chat_id, e)


# ============== CALLBACK: obunani qayta tekshirish ==============
@dp.callback_query(F.data == "check_subs")
async def recheck_subs(cb: CallbackQuery, state: FSMContext):
    not_sub = await check_subscription(cb.from_user.id)
    if not_sub:
        return await cb.message.edit_text(
            "⚠️ Hali barcha kanallarga obuna bo'lmagansiz.\nQuyidagilarga obuna bo'ling va qayta tekshiring.",
            reply_markup=sub_required_markup(not_sub)
        )
    await cb.message.edit_text("✅ Obuna tasdiqlandi! Davom etamiz…")
    await cmd_start(cb.message, state)


# ============== /start ==============
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    if message.chat.type != "private":
        return

    user_id = message.from_user.id

    ref_id = None
    payload = (message.text or "").replace("/start", "", 1).strip()

    state_data = await state.get_data()
    saved_ref_id = state_data.get("referral_id")

    if payload:
        first = payload.split()[0]
        if first.startswith("ref_"):
            cleaned = first.split("\n")[0].split("?")[0].split("@")[0]
            try:
                ref_id = int(cleaned.replace("ref_", ""))
            except:
                ref_id = None
        elif first.isdigit():
            ref_id = int(first)

    if ref_id == user_id:
        ref_id = None

    if ref_id is None and saved_ref_id:
        ref_id = saved_ref_id

    if ref_id:
        await state.update_data(referral_id=ref_id)

    existing_before = await get_user(user_id)
    is_new_user = existing_before is None

    if is_new_user:
        await add_user(user_id, message.from_user.username, ref_id)
        
        if ref_id:
            actual_ref = await get_ref_by(user_id)
            if actual_ref and actual_ref == ref_id:
                try:
                    created = await create_referral(ref_id, user_id)
                    if created:
                        try:
                            invited_label = format_user_short(
                                message.from_user.first_name,
                                message.from_user.username
                            )
                            reward = await get_referral_reward()

                            await bot.send_message(
                                ref_id,
                                "🧲 <b>A’lo! Yangi foydalanuvchi jalb qilindi</b>\n\n"
                                f"🎮 Siz taklif qilgan <b>{invited_label}</b> botga muvaffaqiyatli qo‘shildi.\n\n"
                                "⏳ Endi faqat <b>2 ta qadam</b> qoldi:\n"
                                "• Kanalga a’zo bo‘lish\n"
                                "• Telefon raqamni tasdiqlash\n\n"
                                f"💎 Ushbu jarayon yakunlangach, siz <b>{reward} so'm</b>ga ega bo‘lasiz.\n"
                                "⚡ Do‘stlaringiz harakati — sizning foydangiz!",
                                parse_mode="HTML"
                            )
                        except Exception as e:
                            log.warning("Referral 1-xabar error: %s", e)
                except Exception as e:
                    log.warning("Referral creation error: %s", e)
    else:
        await add_user(user_id, message.from_user.username, None)

    not_sub = await check_subscription(user_id)
    if not_sub:
        await message.answer(
            "📣 Botdan foydalanish uchun quyidagi kanallarga obuna bo'ling.\n"
            "Tayyor bo'lsangiz, <b>✅ Tekshirish</b> tugmasini bosing.",
            parse_mode="HTML",
            reply_markup=sub_required_markup(not_sub),
        )
        return

    current_state = await state.get_state()
    if current_state == VerifyStates.PHONE.state:
        await message.answer(
            "📱 Iltimos, telefon raqamingizni ulashing.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text='📱 Kontaktni ulashish', request_contact=True)]],
                resize_keyboard=True
            )
        )
        return

    verified = await is_verified(user_id)
    if not verified:
        await state.set_state(VerifyStates.PHONE)
        await message.answer(
            "📞 Davom etish uchun telefon raqamingizni tasdiqlang.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="📱 Kontaktni ulashish", request_contact=True)],
                    [KeyboardButton(text="⬅️ Orqaga")]
                ],
                resize_keyboard=True
            ),
        )
        return

    await state.clear()
    await set_menu_state(state, "admin", "main")
    
    await set_menu_state(state, "main", None)
    await bot.send_message(
    chat_id=message.chat.id,
    text=(
         f"🎉 <b>Tabriklaymiz, {message.from_user.first_name}!</b>\n\n"
        "✅ Siz barcha tekshiruvlardan muvaffaqiyatli o‘tdingiz.\n"
        "🚀 Endi botning barcha imkoniyatlari siz uchun ochiq!\n\n"
        "💎 Pul to‘plang, profilingizni rivojlantiring va mukofotlarni qo‘lga kiriting.\n\n"
        "👇 Quyidagi menyudan boshlang:"
    ),
    reply_markup=main_menu,
    parse_mode="HTML"
    )


# ==================== TELEFON VERIFICATION ====================
@dp.message(VerifyStates.PHONE, F.content_type == ContentType.CONTACT)
async def phone_contact_ok(message: Message, state: FSMContext):
    user_id = message.from_user.id
    contact = message.contact

    if not contact or contact.user_id != user_id:
        await message.answer(
            "⚠️ Iltimos, faqat o'z raqamingizni ulashing.\n"
            "Pastdagi '📱 Kontaktni ulashish' tugmasidan foydalaning."
        )
        return

    phone = (contact.phone_number or "").strip()
    phone = phone if phone.startswith("+") else "+" + phone
    

    await set_phone_verified(user_id, phone)
    await set_verified(user_id)

    ref_by = await get_ref_by(user_id)
    # ---------------- 2-XABAR + BONUS (faqat 1 marta) ----------------
    if ref_by and ref_by != user_id:
        # mark_referral_verified returns True only when we actually transitioned to verified
        did_verify = await mark_referral_verified(user_id)
        if did_verify:
            # Step 2: Add achko reward (with error handling)
            try:
                reward = await get_referral_reward()
                await add_almaz(ref_by, reward)
            except Exception as e:
                log.warning("So'm qo'shishda xatolik: %s", e)

            # Step 3: Update rank (with error handling)
            try:
                await update_user_rank(ref_by, with_notification=True)
            except Exception as e:
                log.warning("Rank yangilashda xatolik: %s", e)

            # Step 4: Send 2nd message (SEPARATE try-catch to ensure it runs)
            try:
                total = await count_verified_referrals(ref_by)
                invited_label = format_user_short(
                    message.from_user.first_name or "Foydalanuvchi",
                    message.from_user.username
                )

                txt = (
                    "🎊 <b>Mukofot tayyor! Zo‘r ishladingiz</b>\n\n"
                    f"✅ Siz taklif qilgan <b>{invited_label}</b> barcha tekshiruvlardan muvaffaqiyatli o‘tdi.\n\n"
                    f"💎 Hisobingizga <b>{reward} so'm</b> muvaffaqiyatli qo‘shildi!\n"
                    f"👥 Tasdiqlangan takliflaringiz soni: <b>{total}</b>\n\n"
                    "🔥 Qanchalik ko‘p do‘st taklif qilsangiz — shunchalik tez kuchli mukofotlarga yetasiz.\n"
                    "🚀 Davom eting, imkoniyat siz tomonda!"

                )

                await bot.send_message(ref_by, txt, parse_mode="HTML")
                log.info(f"✅ 2-xabar muvaffaqiyatli yuborildi: {ref_by}")

            except Exception as e:
                # Even if message fails, achko is already added
                log.error(f"❌ 2-xabar yuborishda xatolik (ref_by={ref_by}): {e}")

    await state.clear()

    await message.answer(
        "🎉 Telefon raqamingiz tasdiqlandi!\nEndi botdan to'liq foydalanishingiz mumkin.",
        reply_markup=main_menu
    )


@dp.message(VerifyStates.PHONE)
async def phone_contact_waiting(message: Message, state: FSMContext):
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Kontaktni ulashish", request_contact=True)],
            [KeyboardButton(text="⬅️ Orqaga")]
        ],
        resize_keyboard=True
    )
    await message.answer(
        "⚠️ Raqam qo'lda yozilmadi.\n"
        "Iltimos, pastdagi <b>📱 Kontaktni ulashish</b> tugmasidan foydalaning.",
        parse_mode="HTML", reply_markup=kb
    )


# ============== HELP ==============
@dp.message(Command("help"))
async def user_help(message: Message):
    await message.answer(
        "🆘 <b>Yordam</b>\n\n"
        "Almaz yoki UC — Do'st chaqirish orqali Pul\n"
        "👤 Mening Profilim — Profil va balans ma'lumotlari\n"
        "🏆 Reyting — Top 15 foydalanuvchi (pul bo‘yicha)\n"
        "📣 Yangiliklar & Bonuslar — E'lonlar",
        parse_mode="HTML"
    )


# ============== Profil / Reyting / achko / News / Buy ==============
@dp.message(F.text == "👤 Mening Profilim")
async def show_profile(message: Message, state: FSMContext):
    if await guard_common(message):
        return
    user = await get_user(message.from_user.id)
    if not user:
        await add_user(message.from_user.id, message.from_user.username or "Noma'lum")
        user = await get_user(message.from_user.id)

    almaz = user[3] if len(user) > 3 and user[3] is not None else 0
    total_refs = await count_verified_referrals(message.from_user.id)
    username = user[1] or (message.from_user.username or "Anonim")

    rank, total_users = await get_user_rank(message.from_user.id)

    text = (
        "👤 <b>Profilingiz</b>\n\n"
        f"🆔 Sizning ID: <code>{message.from_user.id}</code>\n"
        f"👤 Username: @{username or 'Anonim'}\n"
        f"📊 Reyting o‘rni: {rank}/{total_users}\n"
        f"💎 Ballar soni: {almaz} so'm\n"
        f"🤝 Umumiy tasdiqlangan takliflar: {total_refs}\n\n"
        "➖ Almaz yoki UC ni istalgan vaqtda yechib olishingiz mumkin"
    )

    await set_menu_state(state, "profile", "main")
    await message.answer(text, parse_mode="HTML", reply_markup=back_kb)


@dp.message(F.text.in_(["🏆 Reyting", "🏅 Top Foydalanuvchilar"]))
async def show_leaderboard_handler(message: Message, state: FSMContext):
    if await guard_common(message):
        return
    leaders = await get_leaderboard(limit=15)
    if not leaders:
        return await message.answer("📉 Hozircha reyting bo'sh.")
    text = "🏆 <b>Top 15 foydalanuvchi (uzs bo‘yicha)</b>\n\n"
    for i, (username, almaz) in enumerate(leaders[:15], 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "⭐"
        text += f"{medal} @{(username or 'Anonim')} — 💎 {almaz}\n"
    await set_menu_state(state, "rating", "main")
    await message.answer(text, parse_mode="HTML", reply_markup=back_kb)


@dp.message(F.text == "🕷️ Free Fire")
async def free_fire_menu(message: Message, state: FSMContext):
    if await guard_common(message):
        return
    data = await state.get_data()
    prev_menu = data.get("menu_current") or "main"
    await send_game_offers(message, state, "ff", prev_menu)


@dp.message(F.text == "〽️ PUBG")
async def pubg_menu(message: Message, state: FSMContext):
    if await guard_common(message):
        return
    data = await state.get_data()
    prev_menu = data.get("menu_current") or "main"
    await send_game_offers(message, state, "pubg", prev_menu)


@dp.callback_query(F.data == PROOF_CHANNEL_BUTTON)
async def proof_channel_button_handler(cb: CallbackQuery):
    channel_value = await get_proof_channel_value()
    if not channel_value:
        await cb.answer("❌ Isbotlar kanali hali sozlanmagan.", show_alert=True)
        return

    url = build_proof_channel_url(channel_value)
    if url:
        await cb.message.answer(f"📜 Isbotlar kanali: {url}")
        await cb.answer("🔗 Havola yuborildi.", show_alert=True)
        return

    await cb.answer("ℹ️ Kanal maxfiy formatda saqlangan. Havola mavjud emas.", show_alert=True)


@dp.message(F.text.in_(["💸 Pul ishlash", "💎 Almaz Topish"]))
async def earn_almaz(message: Message, state: FSMContext):
    if await guard_common(message):
        return

    me = await bot.get_me()
    bot_username = me.username
    user_id = message.from_user.id
    reward = await get_referral_reward()

    html_text = (
        "🎯 <b>Pul Topish — tez, bepul va samarali</b>\n\n"
        "💎 Bu bo‘limda siz hech qanday sarmoyasiz pul yig‘asiz.\n"
        "Faqat do‘stlaringizni taklif qiling — qolganini tizim bajaradi.\n\n"

        "⚙️ <b>Jarayon juda oddiy:</b>\n"
        f"Do‘st → Bot → Tasdiq → Sizga <b>{reward} so'm</b> 💎\n\n"

        "🔗 <b>Sizning shaxsiy taklif havolangiz:</b>\n"
        f"https://t.me/{bot_username}?start=ref_{user_id}\n\n"

        "🔥 Ko‘proq do‘st = ko‘proq kuch.\n"
        "Bugunoq boshlang va farqni his qiling!"

    )

    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💳 Pulni yechib olish")],
            [KeyboardButton(text="⬅️ Orqaga")]
        ],
        resize_keyboard=True
    )
    await set_menu_state(state, "referral", "main")
    await message.answer(html_text, parse_mode="HTML", reply_markup=kb)


@dp.message(F.text == "📣 Yangiliklar & Bonuslar")
async def show_news(message: Message, state: FSMContext):
    if await guard_common(message):
        return
    content = await get_dynamic_text("news")
    if not content:
        return await message.answer("🔭 Hozircha yangiliklar yo'q.")
    if content.startswith("MSG:"):
        try:
            _, chat_id_str, msg_id_str = content.split(":")
            await set_menu_state(state, "news", "main")
            await bot.copy_message(
                message.chat.id,
                int(chat_id_str),
                int(msg_id_str),
                reply_markup=back_kb
            )
            return
        except Exception as e:
            log.warning("copy news failed: %s", e)
    await set_menu_state(state, "news", "main")
    await message.answer(f"📰 <b>So'nggi yangiliklar</b>\n\n{content}", parse_mode="HTML", reply_markup=back_kb)


@dp.message(F.text.in_(["🛒 achko do'koni", "🛒 Almaz Do'koni"]))
async def buy_almaz(message: Message, state: FSMContext):
    if await guard_common(message):
        return
    text = await get_dynamic_text("almaz_buy")
    if not text:
        text = (
            "🛒 <b>do'kon</b>\n\n"
            "To'lov qilganingizdan so'ng, <b>Pul Toldirish</b> bo'limiga o'ting."
        )
    await set_menu_state(state, "shop", "main")
    await message.answer(text, parse_mode="HTML", reply_markup=back_kb)


@dp.message(F.text.in_(["💰 Pul Toldirish", "➕ achko sotib olish", "➕ Achko sotib olish"]))
async def purchase_prompt(message: Message, state: FSMContext):
    if await guard_common(message):
        return
    await state.set_state(PurchaseStates.WAITING_PROOF)
    template = await get_dynamic_text("achko_purchase")
    card_number, card_holder = await get_payment_info()
    if not template:
        template = (
            "🟢 Achko sotib olish\n\n"
            "📌 To‘lov qilish uchun karta:\n"
            "{card_number}\n"
            "👤 {card_holder}\n\n"
            "🆔 Sizning ID: {user_id}\n"
            "(Iltimos, to‘lov qilayotganda ID ni o‘zgartirmang)\n\n"
            "📤 To‘lovdan keyin\n\n"
            "To‘lovni amalga oshirgach, chekni rasm yoki fayl shaklida yuboring:\n\n"
            "📸 Screenshot (skrinshot)\n\n"
            "📄 PDF yoki boshqa fayl\n\n"
            "⚠️ Muhim:\n\n"
            "Summani matn ko‘rinishida yozish shart emas\n\n"
            "Admin chekni ko‘rib, qancha achko qo‘shish kerakligini o‘zi aniqlaydi\n\n"
            "❗ Agar chek yubora olmasangiz yoki muammo bo‘lsa,\n"
            "👉 @username_support ga murojaat qiling"
        )
    await message.answer(
        format_purchase_text(template, message.from_user.id, card_number, card_holder),
        reply_markup=back_kb
    )
    copy_kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="📋 Kartani nusxalash", callback_data="copy_card")]]
    )
    await message.answer("Karta raqamini nusxalash:", reply_markup=copy_kb)


@dp.message(StateFilter(PurchaseStates.WAITING_PROOF), F.content_type.in_({ContentType.PHOTO, ContentType.DOCUMENT}))
async def purchase_receive_proof(message: Message, state: FSMContext):
    if (message.text or "").strip() == "⬅️ Orqaga":
        await state.clear()
        return await message.answer("⏎ Sotib olish bekor qilindi.", reply_markup=main_menu)

    user_id = message.from_user.id
    amount = 0

    # forward/copy message to proof channel if configured
    channel_value = await get_proof_channel_value()
    proof_chat = resolve_proof_chat_id(channel_value)
    proof_msg_id = None
    proof_chat_id = None
    try:
        if proof_chat:
            sent = None
            try:
                if message.photo:
                    sent = await bot.copy_message(chat_id=proof_chat, from_chat_id=message.chat.id, message_id=message.message_id)
                else:
                    sent = await bot.copy_message(chat_id=proof_chat, from_chat_id=message.chat.id, message_id=message.message_id)
            except Exception:
                sent = None
            if sent:
                proof_msg_id = sent.message_id
                proof_chat_id = proof_chat
    except Exception:
        proof_msg_id = None
        proof_chat_id = None

    # create purchase record for admins to process
    try:
        purchase_id = await create_purchase(user_id, amount, proof_chat_id or 0, proof_msg_id or 0)
    except Exception:
        purchase_id = None

    await state.clear()
    await set_menu_state(state, "admin", "main")
    await message.answer(
        "✅ Chek qabul qilindi. Tez orada adminlar tekshiradi va pul hisobingizga qo'shiladi.",
        reply_markup=main_menu
    )
    # notify admins
    admins = [OWNER_ID, OWNER2_ID]
    extra = await list_admins()
    for uid, _ in extra:
        if uid not in admins:
            admins.append(uid)
    for aid in admins:
        try:
            await bot.copy_message(chat_id=aid, from_chat_id=message.chat.id, message_id=message.message_id)
            await bot.send_message(
                aid,
                f"🧾 Yangi sotib olish talabi: ID {user_id} — summa: ko'rsatilmagan — purchase_id: {purchase_id}"
            )
        except Exception:
            pass


@dp.callback_query(F.data == "copy_card")
async def copy_card_callback(cb: CallbackQuery):
    card_number, _ = await get_payment_info()
    await cb.message.answer(f"💳 Karta raqami:\n<code>{card_number}</code>", parse_mode="HTML")
    await cb.answer("Karta raqami yuborildi.")


@dp.message(StateFilter(PurchaseStates.WAITING_PROOF))
async def purchase_invalid_proof(message: Message, state: FSMContext):
    if (message.text or "").strip() == "⬅️ Orqaga":
        await state.clear()
        return await message.answer("⏎ Sotib olish bekor qilindi.", reply_markup=main_menu)
    await message.answer(
        "Iltimos, chekni rasm yoki fayl shaklida yuboring.\n"
        "📸 Screenshot yoki 📄 PDF.",
        reply_markup=back_kb
    )


# ============== achko YECHIB OLISH ==============
@dp.message(F.text.in_(["💳 Pulni yechib olish", "💳 Almazni yechish"]))
async def withdraw_start_message(message: Message, state: FSMContext):
    if await guard_common(message):
        return
    data = await state.get_data()
    prev_menu = data.get("menu_current") or "main"
    await open_withdraw_menu(message, state, prev_menu)


@dp.callback_query(F.data == "withdraw_start")
async def withdraw_start_cb(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    prev_menu = data.get("menu_current") or "main"
    await open_withdraw_menu(cb.message, state, prev_menu)
    await cb.answer()


@dp.callback_query(F.data == "wd_back_prev")
async def withdraw_back_prev(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    prev_menu = data.get("withdraw_prev_menu") or "main"
    await state.clear()
    await render_menu(prev_menu, cb.message, state)
    await cb.answer()


@dp.callback_query(F.data == "wd_back_menu")
async def withdraw_back_menu(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    prev_menu = data.get("withdraw_prev_menu") or "main"
    await open_withdraw_menu(cb.message, state, prev_menu)
    await cb.answer()


@dp.callback_query(F.data.startswith("wd_amount:"))
async def withdraw_choose_amount(cb: CallbackQuery, state: FSMContext):
    user_id = cb.from_user.id
    user = await get_user(user_id)
    if not user:
        await cb.answer("Foydalanuvchi topilmadi.", show_alert=True)
        return
    balance = user[3] if len(user) > 3 and user[3] is not None else 0
    try:
        amount = int(cb.data.split(":")[1])
    except ValueError:
        await cb.answer("Noto'g'ri miqdor.", show_alert=True)
        return

    if balance < amount:
        await cb.answer("Balansingiz o'zgargan, yechish uchun yetarli emas.", show_alert=True)
        return

    await state.set_state(WithdrawStates.WAITING_FF_ID)
    await state.update_data(withdraw_amount=amount, game="ff")

    await cb.message.answer(
        "🎮 Endi, yechib olmoqchi bo'lgan akkauntingiz <b>ID raqamini</b> yuboring.\n\n"
        "ID ni diqqat bilan tekshirib yuboring — pul aynan shu akkauntga tushiriladi.",
        parse_mode="HTML"
    )
    await cb.answer()



@dp.callback_query(F.data.startswith("wd_game:"))
async def withdraw_game_selected(cb: CallbackQuery, state: FSMContext):
    game = cb.data.split(":", 1)[1]
    # load offers for this game
    try:
        offers = await list_offers(game)
    except Exception:
        offers = []

    buttons = []
    if offers:
        for oid, label, cost in offers:
            buttons.append([InlineKeyboardButton(text=f"{label} — {cost} so'm", callback_data=f"wd_offer:{oid}")])

    if not buttons:
        await cb.answer("Hozircha takliflar yo'q yoki balans yetarli emas.", show_alert=True)
        return

    buttons.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="wd_back_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await cb.message.answer("Iltimos, yechib olmoqchi bo'lgan taklifni tanlang:", reply_markup=kb)
    await cb.answer()


@dp.callback_query(F.data.startswith("wd_offer:"))
async def withdraw_offer_selected(cb: CallbackQuery, state: FSMContext):
    try:
        offer_id = int(cb.data.split(":", 1)[1])
    except Exception:
        return await cb.answer("Noto'g'ri taklif.", show_alert=True)

    offer = await get_offer(offer_id)
    if not offer:
        return await cb.answer("Taklif topilmadi.", show_alert=True)

    _, game, label, achko_cost = offer
    user = await get_user(cb.from_user.id)
    balance = user[3] if len(user) > 3 and user[3] is not None else 0
    if balance < achko_cost:
        return await cb.answer("Balansingiz yetarli emas.", show_alert=True)

    # store offer and wait for account id
    await state.set_state(WithdrawStates.WAITING_FF_ID)
    await state.update_data(offer_id=offer_id, withdraw_amount=achko_cost, game=game)

    await cb.message.answer(
        f"Siz tanladingiz: {label} — {achko_cost} som.\n"
        "Endi, yechib olmoqchi bo'lgan akkauntingiz ID raqamini yuboring (Free Fire yoki UC):",
        parse_mode="HTML"
    )
    await cb.answer()


@dp.message(WithdrawStates.WAITING_FF_ID)
async def withdraw_receive_ff_id(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    amount = data.get("withdraw_amount")
    game = data.get("game") or "ff"
    ff_id = (message.text or "").strip()

    if ff_id == "⬅️ Orqaga":
        prev_menu = data.get("withdraw_prev_menu") or "main"
        await state.clear()
        return await open_withdraw_menu(message, state, prev_menu)

    if not amount or not ff_id:
        await message.answer("❌ Noto'g'ri ma'lumot. /start yuborib qayta urinib ko'ring.")
        await state.clear()
        return

    if len(ff_id) < 1:
        await message.answer("⚠️ ID juda qisqa ko'rinmoqda. Iltimos, qayta tekshirib yuboring.")
        return

    # create withdraw request and deduct atomically
    req_id = await create_withdraw_and_deduct(user_id, int(amount), ff_id, game=game)
    if not req_id:
        await message.answer("❌ Balansingiz yetarli emas yoki xatolik yuz berdi.")
        await state.clear()
        return

    await state.clear()

    await message.answer(
        "✅ pul yechish bo'yicha so'rovingiz qabul qilindi!\n\n"
        f"🔥 Miqdor: <b>{amount} som</b>\n"
        f"🎮 Akkaunt ID: <code>{ff_id}</code>\n\n"
        "So'rov adminlarga yuborildi. Iltimos, sabr qiling 🙂",
        parse_mode="HTML"
    )

    await notify_admins_about_withdraw(req_id)
    await send_withdraw_request_to_proof_channel(req_id)


# ============== Withdraw admin callbacklari ==============
@dp.callback_query(F.data.startswith("wd_ok:"))
async def withdraw_approve(cb: CallbackQuery):
    if not await is_owner_or_admin(cb.from_user.id):
        await cb.answer("Sizda bu amal uchun ruxsat yo'q.", show_alert=True)
        return
    try:
        req_id = int(cb.data.split(":")[1])
    except ValueError:
        await cb.answer("Noto'g'ri so'rov.", show_alert=True)
        return

    req = await get_withdraw_request(req_id)
    if not req:
        await cb.answer("So'rov topilmadi yoki o'chirib yuborilgan.", show_alert=True)
        return

    r_id, user_id, amount, ff_id, game, status, created_at, processed_at, processed_by, note = req
    if status != "pending":
        await cb.answer(f"Bu so'rov allaqachon '{status}' holatida.", show_alert=True)
        return

    user = await get_user(user_id)
    balance = user[3] if user and len(user) > 3 and user[3] is not None else 0
    # If balance still has amount, deduct now (old-style requests).
    if balance >= amount:
        await add_almaz(user_id, -amount)
    await update_withdraw_status(req_id, "approved", cb.from_user.id, None)
    await update_withdraw_admin_messages(req_id, "✅ Tasdiqlandi")

    try:
        await bot.send_message(
            user_id,
            "🎉 Pul yechish so'rovingiz tasdiqlandi!\n\n"
            f"💎 Miqdor: <b>{amount} so'm</b>\n"
            "Mukofotingiz hisobingizga muvaffaqiyatli tashlab berildi. Rahmat! 😊",
            parse_mode="HTML"
        )
    except Exception:
        pass

    await send_proof_receipt(req_id, user_id, amount, ff_id, game)
    await cb.answer("So'rov tasdiqlandi.")


@dp.callback_query(F.data.startswith("wd_reject:"))
async def withdraw_reject(cb: CallbackQuery):
    if not await is_owner_or_admin(cb.from_user.id):
        await cb.answer("Sizda bu amal uchun ruxsat yo'q.", show_alert=True)
        return
    try:
        req_id = int(cb.data.split(":")[1])
    except ValueError:
        await cb.answer("Noto'g'ri so'rov.", show_alert=True)
        return

    req = await get_withdraw_request(req_id)
    if not req:
        await cb.answer("So'rov topilmadi yoki o'chirib yuborilgan.", show_alert=True)
        return

    r_id, user_id, amount, ff_id, game, status, created_at, processed_at, processed_by, note = req
    if status != "pending":
        await cb.answer(f"Bu so'rov allaqachon '{status}' holatida.", show_alert=True)
        return

    await update_withdraw_status(req_id, "rejected", cb.from_user.id, None)
    await update_withdraw_admin_messages(req_id, "❌ Rad etildi")

    try:
        await bot.send_message(
            user_id,
            "❌ Pul yechish bo'yicha so'rovingiz rad etildi.\n\n"
            "Bunga turli sabablar bo'lishi mumkin (qoidabuzarlik, noto'g'ri ma'lumot va hokazo).\n"
            "Agar bu xatolik deb o'ylasangiz, qo'llab-quvvatlashga murojaat qilishingiz mumkin.",
            parse_mode="HTML"
        )
    except Exception:
        pass

    await cb.answer("So'rov rad etildi.")


@dp.callback_query(F.data.startswith("wd_edit:"))
async def withdraw_edit_start(cb: CallbackQuery, state: FSMContext):
    if not await is_owner_or_admin(cb.from_user.id):
        await cb.answer("Sizda bu amal uchun ruxsat yo'q.", show_alert=True)
        return
    try:
        req_id = int(cb.data.split(":")[1])
    except ValueError:
        await cb.answer("Noto'g'ri so'rov.", show_alert=True)
        return

    req = await get_withdraw_request(req_id)
    if not req:
        await cb.answer("So'rov topilmadi yoki o'chirib yuborilgan.", show_alert=True)
        return
    if req[5] != "pending":
        await cb.answer(f"Bu so'rov allaqachon '{req[5]}' holatida.", show_alert=True)
        return

    await state.set_state(WithdrawEdit.WAITING_TEXT)
    await state.update_data(edit_request_id=req_id)

    await cb.message.answer(
        "✏️ Foydalanuvchiga yuboriladigan xabar matnini kiriting.\n"
        "Masalan: Free Fire ID noto'g'ri ko'rsatilgan, iltimos, qayta yuboring.",
        reply_markup=back_kb
    )
    await cb.answer()


@dp.message(WithdrawEdit.WAITING_TEXT)
async def withdraw_edit_send(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    data = await state.get_data()
    req_id = data.get("edit_request_id")
    if not req_id:
        await state.clear()
        await message.answer("❌ So'rov topilmadi. Qaytadan urinib ko'ring.", reply_markup=admin_menu)
        return

    note = (message.text or "").strip()
    req = await get_withdraw_request(int(req_id))
    if not req:
        await state.clear()
        await message.answer("❌ So'rov bazadan topilmadi.", reply_markup=admin_menu)
        return

    r_id, user_id, amount, ff_id, game, status, created_at, processed_at, processed_by, old_note = req
    if status != "pending":
        await state.clear()
        await message.answer(f"ℹ️ Bu so'rov allaqachon '{status}' holatiga o'tkazilgan.", reply_markup=admin_menu)
        return

    try:
        await bot.send_message(
            user_id,
            f"✏️ Pul yechish so'rovi bo'yicha xabar:\n\n{note}"
        )
    except Exception:
        pass

    await update_withdraw_status(int(req_id), "edited", message.from_user.id, note)
    await update_withdraw_admin_messages(int(req_id), "✏️ Tahrirlandi")

    await state.clear()
    await message.answer(
        "✅ Izoh foydalanuvchiga yuborildi va so'rov <b>tahrirlandi</b> holatiga o'tkazildi.",
        parse_mode="HTML",
        reply_markup=admin_menu
    )

# ============== ADMIN PANEL ==============
@dp.message(Command("admin"))
async def admin_panel(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return await message.answer("🚫 Siz admin emassiz.")
    await set_menu_state(state, "admin", "main")
    await message.answer("👑 <b>Admin panel</b>", parse_mode="HTML", reply_markup=admin_menu)


@dp.message(F.text == "📊 Foydalanuvchilar soni")
async def user_count(message: Message):
    if not await is_owner_or_admin(message.from_user.id):
        return
    from database import DB_NAME
    import aiosqlite
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT COUNT(*) FROM users")
        total = (await cur.fetchone())[0]
    await message.answer(f"📈 Jami foydalanuvchilar: <b>{total}</b>", parse_mode="HTML")


@dp.message(F.text == "📜 Isbotlar kanali")
async def proof_channel_prompt(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return

    current = await get_proof_channel_value()
    current_text = f"<code>{current}</code>" if current else "❌ Sozlanmagan"
    info = (
        "📜 <b>Isbotlar kanali</b>\n\n"
        "Bot tasdiqlangan Pul yechish cheklarining nusxasini shu kanalda joylaydi.\n"
        "Botni kanalga admin qiling va <b>@username</b> yoki <b>-100...</b> ID yuboring.\n"
        "Sozlamani o'chirish uchun <b>0</b> yuboring.\n"
        "Bekor qilish uchun <b>⬅️ Orqaga</b> tugmasidan foydalaning.\n\n"
        f"Hozirgi qiymat: {current_text}\n"
        "ℹ️ Kanal maxfiy ID bo'lsa, foydalanuvchilar uchun tugma havola bera olmaydi."
    )
    await state.set_state(ProofChannelSetup.WAITING)
    await message.answer(info, parse_mode="HTML", reply_markup=back_kb)


@dp.message(F.text == "⬅️ Orqaga", StateFilter(ProofChannelSetup.WAITING))
async def proof_channel_cancel(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await state.clear()
    await set_menu_state(state, "admin", "main")
    await message.answer("⏎ Isbotlar kanali sozlamalari bekor qilindi.", reply_markup=admin_menu)


@dp.message(StateFilter(ProofChannelSetup.WAITING))
async def proof_channel_save(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    raw = (message.text or "").strip()
    try:
        normalized = normalize_proof_channel_value(raw)
    except ValueError as exc:
        await message.answer(f"❌ {exc}", reply_markup=back_kb)
        return

    await state.clear()
    if normalized is None:
        await delete_setting(PROOF_CHANNEL_SETTING_KEY)
        await set_menu_state(state, "admin", "main")
        await message.answer("ℹ️ Isbotlar kanali o'chirildi.", reply_markup=admin_menu)
        return

    await set_setting(PROOF_CHANNEL_SETTING_KEY, normalized)
    url_hint = build_proof_channel_url(normalized)
    extra = f"\n🔗 Havola: {url_hint}" if url_hint else "\nℹ️ Kanal ID ko'rinishida saqlandi."
    await set_menu_state(state, "admin", "main")
    await message.answer(
        "✅ Isbotlar kanali yangilandi.\n"
        f"Saqlangan qiymat: <code>{normalized}</code>{extra}",
        parse_mode="HTML",
        reply_markup=admin_menu
    )


@dp.message(F.text == "📰 Reklama/Yangilik sozlash")
async def edit_news(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await state.set_state(TextEdit.new_text)
    await state.update_data(section="news")
    await message.answer("📰 Yangi yangilik xabarini yuboring (matn yoki media).", reply_markup=back_kb)


@dp.message(F.text.in_(["💰 achko sotib olish matni", "💰 Almaz sotib olish matni"]))
async def edit_buy_text(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await state.set_state(TextEdit.new_text)
    await state.update_data(section="almaz_buy")
    await message.answer("💰 Achko do'koni bo'limi uchun matn yuboring:", reply_markup=back_kb)


@dp.message(F.text == "🟢 achko sotib olish matni")
async def edit_purchase_text(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await state.set_state(TextEdit.new_text)
    await state.update_data(section="achko_purchase")
    await message.answer(
        "🟢 Achko sotib olish bo'limi uchun matn yuboring.\n"
        "ID: <code>{user_id}</code> yoki <code>{user.id}</code>\n"
        "Karta: <code>{card_number}</code>\n"
        "Ism: <code>{card_holder}</code>",
        parse_mode="HTML",
        reply_markup=back_kb
    )


@dp.message(F.text == "💳 Karta raqami")
async def card_number_prompt(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await state.set_state(PaymentSetup.CARD)
    await message.answer(
        "Yangi karta raqamini yuboring.\n"
        "O'chirish uchun <b>0</b> yuboring.",
        parse_mode="HTML",
        reply_markup=back_kb
    )


@dp.message(F.text == "👤 Karta egasi")
async def card_holder_prompt(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await state.set_state(PaymentSetup.HOLDER)
    await message.answer(
        "Karta egasi (ism familiya) yuboring.\n"
        "O'chirish uchun <b>0</b> yuboring.",
        parse_mode="HTML",
        reply_markup=back_kb
    )


@dp.message(StateFilter(PaymentSetup.CARD))
async def card_number_save(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    text = (message.text or "").strip()
    if text == "⬅️ Orqaga":
        await state.clear()
        return await message.answer("⏎ Bekor qilindi.", reply_markup=admin_menu)
    if text == "0":
        await delete_setting("card_number")
        await state.clear()
        return await message.answer("✅ Karta raqami o'chirildi (default qaytadi).", reply_markup=admin_menu)
    await set_setting("card_number", text)
    await state.clear()
    await message.answer("✅ Karta raqami yangilandi.", reply_markup=admin_menu)


@dp.message(StateFilter(PaymentSetup.HOLDER))
async def card_holder_save(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    text = (message.text or "").strip()
    if text == "⬅️ Orqaga":
        await state.clear()
        return await message.answer("⏎ Bekor qilindi.", reply_markup=admin_menu)
    if text == "0":
        await delete_setting("card_holder")
        await state.clear()
        return await message.answer("✅ Karta egasi o'chirildi (default qaytadi).", reply_markup=admin_menu)
    await set_setting("card_holder", text)
    await state.clear()
    await message.answer("✅ Karta egasi yangilandi.", reply_markup=admin_menu)


@dp.message(F.text == "⬅️ Orqaga", StateFilter(TextEdit.new_text))
async def cancel_text_edit(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await state.clear()
    await set_menu_state(state, "admin", "main")
    await message.answer("⏎ O'zgartirish bekor qilindi.", reply_markup=admin_menu)


@dp.message(StateFilter(TextEdit.new_text))
async def save_dynamic_text(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    data = await state.get_data()
    section = data.get("section")
    if section == "news":
        if message.text and not message.caption:
            await update_dynamic_text("news", message.text)
        else:
            await update_dynamic_text("news", f"MSG:{message.chat.id}:{message.message_id}")
        await state.clear()
        await set_menu_state(state, "admin", "main")
        return await message.answer("✅ Yangilik xabari yangilandi.", reply_markup=admin_menu)
    if section == "almaz_buy":
        await update_dynamic_text("almaz_buy", message.text or "")
        await state.clear()
        await set_menu_state(state, "admin", "main")
        return await message.answer("✅ Matn yangilandi.", reply_markup=admin_menu)
    if section == "achko_purchase":
        await update_dynamic_text("achko_purchase", message.text or "")
        await state.clear()
        await set_menu_state(state, "admin", "main")
        return await message.answer("✅ Matn yangilandi.", reply_markup=admin_menu)


@dp.message(F.text == "📢 Reklama yuborish")
async def ask_broadcast(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await message.answer("📢 Reklama xabarini yuboring (matn yoki media).", reply_markup=back_kb)
    await state.set_state(Broadcast.WAITING)


@dp.message(F.text == "⬅️ Orqaga", StateFilter(Broadcast.WAITING))
async def cancel_broadcast(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await state.clear()
    await set_menu_state(state, "admin", "main")
    await message.answer("⏎ Reklama yuborish bekor qilindi.", reply_markup=admin_menu)


@dp.message(
    StateFilter(Broadcast.WAITING),
    F.content_type.in_({
        ContentType.TEXT, ContentType.PHOTO, ContentType.VIDEO, ContentType.AUDIO,
        ContentType.DOCUMENT, ContentType.VOICE, ContentType.STICKER, ContentType.VIDEO_NOTE
    })
)
async def handle_broadcast(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer("🚀 Reklama yuborilmoqda… ⏳")

    from database import DB_NAME
    import aiosqlite
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT user_id FROM users")
        users = [r[0] for r in await cur.fetchall()]

    total, success, failed = len(users), 0, 0
    for uid in users:
        try:
            await bot.copy_message(chat_id=uid, from_chat_id=message.chat.id, message_id=message.message_id)
            success += 1
            await asyncio.sleep(0.03)
        except Exception:
            failed += 1

    await message.answer(
        f"✅ Reklama yakunlandi!\n📬 Yuborilgan: <b>{success}</b>\n❌ Yetkazilmagan: <b>{failed}</b>\n👥 Jami: <b>{total}</b>",
        parse_mode="HTML", reply_markup=admin_menu
    )
    await set_menu_state(state, "admin", "main")


@dp.message(F.text == "🧩 Majburiy kanallar")
async def channels_menu(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    channels = await list_required_channels()
    text = "🧩 <b>Majburiy kanallar</b>\n\n" + ("\n".join(f"• {ch}" for ch in channels) if channels else "– Hozircha kanal yo'q.")
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Kanal qo'shish"), KeyboardButton(text="➖ Kanal o'chirish")],
            [KeyboardButton(text="⬅️ Orqaga")]
        ], resize_keyboard=True
    )
    await set_menu_state(state, "channels", "admin")
    await message.answer(text, parse_mode="HTML", reply_markup=kb)


@dp.message(F.text == "🎯 Withdraw takliflar")
async def offers_menu(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    # show offers for both games
    ff_offers = await list_offers("ff")
    pubg_offers = await list_offers("pubg")
    text = "🎯 <b>Withdraw takliflar</b>\n\n"
    text += "<b>Free Fire:</b>\n" + ("\n".join(f"• {o[1]} — {o[2]} som (ID: {o[0]})" for o in ff_offers) if ff_offers else "– Hozircha yo'q.")
    text += "\n\n<b>PUBG:</b>\n" + ("\n".join(f"• {o[1]} — {o[2]} som (ID: {o[0]})" for o in pubg_offers) if pubg_offers else "– Hozircha yo'q.")

    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Taklif qo'shish"), KeyboardButton(text="➖ Taklif o'chirish")],
        [KeyboardButton(text="⬅️ Orqaga")]
    ], resize_keyboard=True)

    await set_menu_state(state, "offers", "admin")
    await message.answer(text, parse_mode="HTML", reply_markup=kb)


@dp.message(F.text == "➕ Taklif qo'shish")
async def offer_add_prompt(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await state.set_state(OfferManage.ADD)
    await message.answer(
        "Yangi taklif qo'shish — format:\n<code>game|label|som_cost</code>\nMasalan: <code>ff|105 Almaz|105</code>",
        parse_mode="HTML",
        reply_markup=back_kb
    )


@dp.message(StateFilter(OfferManage.ADD))
async def offer_add_exec(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    if (message.text or "").strip() == "⬅️ Orqaga":
        await state.clear()
        return await message.answer("⏎ Bekor qilindi.", reply_markup=admin_menu)
    parts = (message.text or "").split("|")
    if len(parts) != 3:
        return await message.answer("⚠️ Format noto'g'ri. Iltimos: game|label|achko_cost")
    game = parts[0].strip()
    label = parts[1].strip()
    try:
        cost = int(parts[2].strip())
    except Exception:
        return await message.answer("⚠️ Cost raqam bo'lishi kerak.")

    await create_offer(game, label, cost)
    await state.clear()
    await set_menu_state(state, "admin", "main")
    await message.answer("✅ Taklif qo'shildi.", reply_markup=admin_menu)


@dp.message(F.text == "➖ Taklif o'chirish")
async def offer_remove_prompt(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await state.set_state(OfferManage.REMOVE)
    await message.answer("O'chirish uchun taklif ID yuboring:", reply_markup=back_kb)


@dp.message(StateFilter(OfferManage.REMOVE))
async def offer_remove_exec(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    if (message.text or "").strip() == "⬅️ Orqaga":
        await state.clear()
        return await message.answer("⏎ Bekor qilindi.", reply_markup=admin_menu)
    if not (message.text or "").strip().isdigit():
        return await message.answer("⚠️ ID raqam bo'lishi kerak.")
    oid = int(message.text.strip())
    ok = await delete_offer(oid)
    await state.clear()
    await set_menu_state(state, "admin", "main")
    await message.answer("✅ O'chirildi." if ok else "ℹ️ Bunday taklif topilmadi.", reply_markup=admin_menu)


@dp.message(F.text == "➕ Kanal qo'shish")
async def channel_add_prompt(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    cnt = await required_channels_count()
    if cnt >= 6:
        return await message.answer("⚠️ 6 tadan ortiq kanal ulash mumkin emas.")
    await state.set_state(ChanManage.ADD)
    await message.answer("Kanal username'ini yuboring (masalan: @mychannel yoki -100... ID):", reply_markup=back_kb)


@dp.message(StateFilter(ChanManage.ADD))
async def channel_add(message: Message, state: FSMContext):
    ch = (message.text or "").strip()
    if not await is_owner_or_admin(message.from_user.id):
        return
    if ch == "⬅️ Orqaga":
        await state.clear()
        return await message.answer("⏎ Bekor qilindi.", reply_markup=admin_menu)
    if not (ch.startswith("@") or ch.startswith("-100") or ch.lstrip("-").isdigit()):
        return await message.answer("⚠️ Iltimos, @username yoki -100... chat ID yuboring.")
    ok = await add_required_channel(ch)
    await state.clear()
    await set_menu_state(state, "admin", "main")
    if ok:
        await message.answer("✅ Kanal qo'shildi.", reply_markup=admin_menu)
    else:
        await message.answer("ℹ️ Bu kanal allaqachon mavjud.", reply_markup=admin_menu)


@dp.message(F.text == "➖ Kanal o'chirish")
async def channel_remove_prompt(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await state.set_state(ChanManage.REMOVE)
    await message.answer("O'chirish uchun @username yoki -100... chat ID yuboring:", reply_markup=back_kb)


@dp.message(StateFilter(ChanManage.REMOVE))
async def channel_remove(message: Message, state: FSMContext):
    ch = (message.text or "").strip()
    if not await is_owner_or_admin(message.from_user.id):
        return
    if ch == "⬅️ Orqaga":
        await state.clear()
        return await message.answer("⏎ Bekor qilindi.", reply_markup=admin_menu)
    ok = await remove_required_channel(ch)
    await state.clear()
    await set_menu_state(state, "admin", "main")
    if ok:
        await message.answer("✅ Kanal o'chirildi.", reply_markup=admin_menu)
    else:
        await message.answer("ℹ️ Bunday kanal topilmadi.", reply_markup=admin_menu)


@dp.message(F.text == "🛡 Admin boshqaruvi")
async def admin_manage_menu(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return await message.answer("🚫 Bu bo'lim faqat egasi uchun.")
    admins = await list_admins()
    text = "🛡 Adminlar ro'yxati:\n" + ("\n".join(f"• @{u or 'unknown'} – {uid}" for uid, u in admins) if admins else "– Hech kim yo'q.")
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="👤 Admin qo'shish"), KeyboardButton(text="🗑 Adminni o'chirish")],
            [KeyboardButton(text="⬅️ Orqaga")]
        ], resize_keyboard=True
    )
    await set_menu_state(state, "admin_manage", "admin")
    await message.answer(text, parse_mode="HTML", reply_markup=kb)


@dp.message(F.text == "👤 Admin qo'shish")
async def admin_add_prompt(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    await state.set_state(AdminManage.ADD)
    await message.answer("Admin qilish uchun foydalanuvchi ID yuboring:", reply_markup=back_kb)


@dp.message(StateFilter(AdminManage.ADD))
async def admin_add_exec(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    if (message.text or "").strip() == "⬅️ Orqaga":
        await state.clear()
        return await message.answer("⏎ Bekor qilindi.", reply_markup=admin_menu)
    if not (message.text or "").isdigit():
        return await message.answer("ID faqat raqamlardan iborat bo'lishi kerak.")
    uid = int(message.text)
    ok = await add_admin(uid, None)
    await state.clear()
    await set_menu_state(state, "admin", "main")
    await message.answer("✅ Admin qo'shildi." if ok else "ℹ️ Bu foydalanuvchi allaqachon admin.", reply_markup=admin_menu)


@dp.message(F.text == "🗑 Adminni o'chirish")
async def admin_remove_prompt(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    await state.set_state(AdminManage.REMOVE)
    await message.answer("O'chirish uchun admin ID yuboring:", reply_markup=back_kb)


@dp.message(StateFilter(AdminManage.REMOVE))
async def admin_remove_exec(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    if (message.text or "").strip() == "⬅️ Orqaga":
        await state.clear()
        return await message.answer("⏎ Bekor qilindi.", reply_markup=admin_menu)
    if not (message.text or "").isdigit():
        return await message.answer("ID faqat raqamlardan iborat bo'lishi kerak.")
    uid = int(message.text)
    ok = await remove_admin(uid)
    await state.clear()
    await set_menu_state(state, "admin", "main")
    await message.answer("✅ Admin o'chirildi." if ok else "ℹ️ Bunday admin topilmadi.", reply_markup=admin_menu)


@dp.message(F.text.in_(["➕ Pul berish", "💎 Qo'lda almaz berish"]))
async def achko_add_prompt(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await state.set_state(AdjustAchko.ADD)
    await message.answer(
        "so'm qo‘shish — format:\n"
        "<code>user_id: 13200</code>\n"
        "Masalan: <code>123456789: 5000</code>",
        parse_mode="HTML",
        reply_markup=back_kb
    )


@dp.message(F.text == "➖ achko olib tashlash")
async def achko_remove_prompt(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await state.set_state(AdjustAchko.REMOVE)
    await message.answer(
        "so'm olib tashlash — format:\n"
        "<code>user_id: 13200</code>\n"
        "Masalan: <code>123456789: 500</code>",
        parse_mode="HTML",
        reply_markup=back_kb
    )


@dp.message(StateFilter(AdjustAchko.ADD))
async def achko_add_exec(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    text = (message.text or "").strip()
    if text == "⬅️ Orqaga":
        await state.clear()
        return await message.answer("⏎ Bekor qilindi.", reply_markup=admin_menu)

    parsed = parse_user_amount(text)
    if not parsed:
        return await message.answer(
            "⚠️ Format noto'g'ri.\n"
            "<code>user_id: 13200</code> ko'rinishida yuboring.",
            parse_mode="HTML"
        )

    user_id, amount = parsed
    if amount <= 0:
        return await message.answer("⚠️ Miqdor musbat bo'lishi kerak.")

    user = await get_user(user_id)
    if not user:
        return await message.answer("❌ Bunday foydalanuvchi bazada topilmadi.")

    ok = await adjust_balance(user_id, amount, min_zero=False)
    if not ok:
        return await message.answer("❌ Balansni yangilashda xatolik.")

    await log_admin_action(message.from_user.id, "achko_add", user_id, amount, None)
    updated = await get_user(user_id)
    balance = updated[3] if updated and len(updated) > 3 and updated[3] is not None else 0
    await state.clear()

    try:
        await bot.send_message(
            user_id,
            f"🎁 Sizga bot administratori tomonidan <b>{amount} som</b> qo'shildi!",
            parse_mode="HTML"
        )
    except Exception:
        pass

    await message.answer(
        f"✅ <code>{user_id}</code> foydalanuvchi hisobiga {amount}som qo'shildi.\n"
        f"💎 Yangi balans: <b>{balance} som</b>",
        parse_mode="HTML",
        reply_markup=admin_menu
    )


@dp.message(StateFilter(AdjustAchko.REMOVE))
async def achko_remove_exec(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    text = (message.text or "").strip()
    if text == "⬅️ Orqaga":
        await state.clear()
        return await message.answer("⏎ Bekor qilindi.", reply_markup=admin_menu)

    parsed = parse_user_amount(text)
    if not parsed:
        return await message.answer(
            "⚠️ Format noto'g'ri.\n"
            "<code>user_id: 13200</code> ko'rinishida yuboring.",
            parse_mode="HTML"
        )

    user_id, amount = parsed
    if amount <= 0:
        return await message.answer("⚠️ Miqdor musbat bo'lishi kerak.")

    user = await get_user(user_id)
    if not user:
        return await message.answer("❌ Bunday foydalanuvchi bazada topilmadi.")

    ok = await adjust_balance(user_id, -amount, min_zero=True)
    if not ok:
        return await message.answer("❌ Balans yetarli emas yoki xatolik yuz berdi.")

    await log_admin_action(message.from_user.id, "achko_remove", user_id, amount, None)
    updated = await get_user(user_id)
    balance = updated[3] if updated and len(updated) > 3 and updated[3] is not None else 0
    await state.clear()

    try:
        await bot.send_message(
            user_id,
            f"⚠️ Hisobingizdan <b>{amount} som</b> olib tashlandi.",
            parse_mode="HTML"
        )
    except Exception:
        pass

    await message.answer(
        f"✅ <code>{user_id}</code> foydalanuvchi hisobidan {amount} som olib tashlandi.\n"
        f"💎 Yangi balans: <b>{balance} som</b>",
        parse_mode="HTML",
        reply_markup=admin_menu
    )


@dp.message(F.text == "💾 Backup yaratish")
async def create_backup(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    result = await backup_database()
    if result.startswith("Backup xatosi"):
        await set_menu_state(state, "admin", "main")
        await message.answer(f"❌ {result}", reply_markup=admin_menu)
    else:
        await set_menu_state(state, "admin", "main")
        await message.answer(
            f"✅ Baza muvaffaqiyatli zaxiralandi!\n📁 Fayl: <code>{result}</code>",
            parse_mode="HTML",
            reply_markup=admin_menu
        )


@dp.message(F.text.in_(["🔧 Referal mukofoti (so'm)", "🔧 Referal almaz qiymati"]))
async def change_ref_reward(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    current = await get_referral_reward()
    await state.set_state("WAITING_REF_REWARD")
    await message.answer(
        f"🔧 Hozirgi referal mukofoti: <b>{current} som</b>\n\n"
        "Yangi qiymatni kiriting (faqat raqam):",
        parse_mode="HTML",
        reply_markup=back_kb
    )


@dp.message(StateFilter("WAITING_REF_REWARD"))
async def save_new_ref_reward(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return

    if message.text.strip() == "⬅️ Orqaga":
        await state.clear()
        return await message.answer("⏎ Bekor qilindi.", reply_markup=admin_menu)

    if not message.text.isdigit():
        return await message.answer("⚠ Iltimos, faqat raqam kiriting.")

    value = message.text.strip()
    await set_setting("referral_reward", value)

    await state.clear()
    await message.answer(
        f"✅ Referal mukofoti yangilandi: <b>{value} som</b>",
        parse_mode="HTML",
        reply_markup=admin_menu
    )


@dp.message(F.text == "📈 Statistika")
async def show_stats(message: Message):
    if not await is_owner_or_admin(message.from_user.id):
        return
    top = await get_top_referrers_today(limit=10)
    total, pending, approved, edited, rejected = await get_withdraw_stats()

    text = "📈 <b>Statistika</b>\n\n"

    text += "🏆 Bugungi TOP-10 taklif qiluvchilar (tasdiqlangan referallar):\n"
    if not top:
        text += "– Hozircha ma'lumot yo'q.\n"
    else:
        for i, (uid, username, cnt) in enumerate(top, 1):
            uname = f"@{username}" if username else f"ID:{uid}"
            text += f"{i}. {uname} – {cnt} ta tasdiqlangan referal\n"

    text += "\n💳 <b>som yechish so'rovlari</b>:\n"
    text += f"• Umumiy so'rovlar: <b>{total}</b>\n"
    text += f"• Tasdiqlangan: <b>{approved}</b>\n"
    text += f"• Tahrirlangan: <b>{edited}</b>\n"
    text += f"• Rad etilgan: <b>{rejected}</b>\n"
    text += f"• Hozirda kutilayotgan: <b>{pending}</b>\n"

    await message.answer(text, parse_mode="HTML")

    if top:
        buttons = []
        for uid, username, cnt in top:
            label = f"{'@'+username if username else str(uid)} – {cnt} ta"
            buttons.append([InlineKeyboardButton(text=label, callback_data=f"topuser:{uid}")])
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        await message.answer(
            "🔍 TOP-10 ichidan foydalanuvchi profilini ko'rish uchun tanlang:",
            reply_markup=kb
        )


@dp.callback_query(F.data.startswith("topuser:"))
async def top_user_profile(cb: CallbackQuery):
    if not await is_owner_or_admin(cb.from_user.id):
        await cb.answer("Siz admin emassiz.", show_alert=True)
        return
    try:
        uid = int(cb.data.split(":")[1])
    except ValueError:
        await cb.answer("Noto'g'ri ID.", show_alert=True)
        return

    user = await get_user(uid)
    if not user:
        await cb.answer("Foydalanuvchi topilmadi.", show_alert=True)
        return

    almaz = user[3] if len(user) > 3 and user[3] is not None else 0
    phone = user[5] if len(user) > 5 else None
    total_refs_verified = await count_verified_referrals(uid)
    remain = await get_suspension_remaining(uid)

    txt = (
        "🔎 <b>Foydalanuvchi profili</b>\n\n"
        f"🆔 ID: <code>{uid}</code>\n"
        f"👤 Username: @{user[1] or 'Anonim'}\n"
        f"📞 Telefon: {phone or '–'}\n"
        f"💎 Balans: {almaz}\n so'm"
        f"🤝 Tasdiqlangan takliflar: {total_refs_verified}\n"
        f"⏳ Tanaffus (qolgan): {remain} s\n"
    )
    await cb.message.answer(txt, parse_mode="HTML")
    await cb.answer()


@dp.message(F.text == "⏳ Tanaffus berish")
async def suspend_prompt(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await state.set_state(SuspensionInput.WAIT)
    await message.answer(
        "🕒 Tanaffus berish: <code>user_id soat</code> ko'rinishida yuboring. Masalan:\n"
        "<code>123456789 2</code>  (2 soatga tanaffus)",
        parse_mode="HTML", reply_markup=back_kb
    )


@dp.message(F.text == "⬅️ Orqaga", StateFilter(SuspensionInput.WAIT))
async def suspend_back_handler(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer("⏪ Tanaffus bo'limidan chiqildi.", reply_markup=admin_menu)


@dp.message(
    StateFilter(SuspensionInput.WAIT),
    lambda m: (m.text or "").strip().count(" ") == 1 and all(p.isdigit() for p in (m.text or "").split())
)
async def simple_two_ints_handler(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    uid_str, hour_str = (message.text or "").split()
    uid, hours = int(uid_str), int(hour_str)
    seconds = hours * 3600
    await set_suspension(uid, seconds)
    remain = await get_suspension_remaining(uid)
    try:
        await bot.send_message(
            uid,
            "😴 Profilingiz vaqtincha tanaffusda.\n"
            f"⏰ Tanaffus {hours} soatga belgilandi. Taxminan {remain} soniyadan so'ng bot qayta faollashadi.",
            parse_mode="HTML"
        )
    except Exception:
        pass
    await state.clear()
    await message.answer(
        f"✅ <code>{uid}</code> foydalanuvchi {hours} soatga tanaffusga chiqarildi.",
        parse_mode="HTML",
        reply_markup=admin_menu
    )


@dp.message(F.text == "🔎 Foydalanuvchini topish")
async def search_user_prompt(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await state.set_state(SearchUser.WAIT)
    await message.answer("ID yoki @username yuboring:", reply_markup=back_kb)


@dp.message(F.text == "⬅️ Orqaga", StateFilter(SearchUser.WAIT))
async def search_user_cancel(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    await state.clear()
    await set_menu_state(state, "admin", "main")
    await message.answer("⏎ Qidiruv bekor qilindi.", reply_markup=admin_menu)


@dp.message(StateFilter(SearchUser.WAIT))
async def search_user_exec(message: Message, state: FSMContext):
    if not await is_owner_or_admin(message.from_user.id):
        return
    from database import DB_NAME
    import aiosqlite
    q = (message.text or "").strip()

    row = None
    async with aiosqlite.connect(DB_NAME) as db:
        if q.isdigit():
            cur = await db.execute(
                "SELECT user_id, username, almaz, ref_by, verified, phone, created_at FROM users WHERE user_id=?",
                (int(q),)
            )
            row = await cur.fetchone()
        elif q.startswith("@"):
            cur = await db.execute(
                "SELECT user_id, username, almaz, ref_by, verified, phone, created_at FROM users WHERE username=?",
                (q.lstrip("@"),)
            )
            row = await cur.fetchone()

        if not row:
            await state.clear()
            return await message.answer("❌ Foydalanuvchi topilmadi.", reply_markup=admin_menu)

        user_id, username, almaz, ref_by, verified, phone, created_at = row
        cur = await db.execute("SELECT COUNT(*) FROM users WHERE ref_by=?", (user_id,))
        ref_cnt = (await cur.fetchone())[0]
    remain = await get_suspension_remaining(user_id)
    first_seen = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(created_at or 0)) if created_at else "–"

    txt = (
        "🔎 <b>Foydalanuvchi ma’lumoti</b>\n\n"
        f"🆔 <b>ID:</b> <code>{user_id}</code>\n"
        f"👤 <b>Username:</b> @{username or 'Anonim'}\n"
        f"📞 <b>Telefon:</b> {phone or '—'}\n"
        f"✅ <b>Verified:</b> {'Ha' if (verified or 0) else 'Yo‘q'}\n"
        f"💎 <b>:<valansi /b> {almaz or 0} som\n"
        f"🤝 <b>Referal soni (ref_by bilan):</b> {ref_cnt}\n"
        f"🕒 <b>Ro‘yxatdan o‘tgan:</b> {first_seen}\n"
        f"⏳ <b>Tanaffus (qolgan):</b> {remain} s\n"
    )
    
    await state.clear()
    await set_menu_state(state, "admin", "main")
    await message.answer(txt, parse_mode="HTML", reply_markup=admin_menu)


@dp.message(F.text == "⬅️ Orqaga")
async def back_to_previous_menu(message: Message, state: FSMContext):
    await go_back_menu(message, state)


@dp.message(F.text == "⬅️ Chiqish")
async def admin_exit_to_main(message: Message, state: FSMContext):
    if await is_owner_or_admin(message.from_user.id):
        await state.clear()
        return await message.answer("🏠 Asosiy menyuga qaytdingiz.", reply_markup=main_menu)
    await state.clear()
    await message.answer("🏠 Asosiy menyuga qaytdingiz.", reply_markup=main_menu)


# Bootstrap
async def main():
    await init_db()
    await setup_bot_commands()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
