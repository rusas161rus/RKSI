const dayNames = {
  monday: "Понедельник",
  tuesday: "Вторник",
  wednesday: "Среда",
  thursday: "Четверг",
  friday: "Пятница",
  saturday: "Суббота"
};

const lessons = [
  { day: "monday", dayRu: "Понедельник", time: "09:00-10:30", subject: "Математика", teacher: "Иванов И.И.", room: "Ауд. 101" },
  { day: "monday", dayRu: "Понедельник", time: "11:00-12:30", subject: "Физика", teacher: "Петрова А.С.", room: "Ауд. 204" },
  { day: "tuesday", dayRu: "Вторник", time: "10:00-11:30", subject: "Программирование", teacher: "Смирнов Д.О.", room: "Лаб. 3" },
  { day: "wednesday", dayRu: "Среда", time: "12:00-13:30", subject: "История", teacher: "Козлова Н.В.", room: "Ауд. 105" },
  { day: "thursday", dayRu: "Четверг", time: "09:30-11:00", subject: "Английский язык", teacher: "Wilson K.", room: "Ауд. 112" },
  { day: "friday", dayRu: "Пятница", time: "13:00-14:30", subject: "Биология", teacher: "Егорова М.П.", room: "Ауд. 220" },
  { day: "saturday", dayRu: "Суббота", time: "11:00-12:30", subject: "Физкультура", teacher: "Андреев О.Н.", room: "Спортзал" }
];

const scheduleRoot = document.getElementById("schedule");
const form = document.getElementById("lesson-form");
const buttons = document.querySelectorAll(".day-btn");
let activeDay = "all";

function renderSchedule(day) {
  const data = day === "all" ? lessons : lessons.filter((lesson) => lesson.day === day);

  if (!data.length) {
    scheduleRoot.innerHTML = '<article class="empty">На этот день занятий нет.</article>';
    return;
  }

  scheduleRoot.innerHTML = data
    .map(
      (lesson) => `
        <article class="card">
          <p class="card__day">${lesson.dayRu}</p>
          <p class="card__time">${lesson.time}</p>
          <h2 class="card__subject">${lesson.subject}</h2>
          <p class="card__meta">${lesson.teacher} • ${lesson.room}</p>
        </article>
      `
    )
    .join("");
}

buttons.forEach((button) => {
  button.addEventListener("click", () => {
    buttons.forEach((btn) => btn.classList.remove("is-active"));
    button.classList.add("is-active");
    activeDay = button.dataset.day;
    renderSchedule(activeDay);
  });
});

form.addEventListener("submit", (event) => {
  event.preventDefault();

  const formData = new FormData(form);
  const day = formData.get("day");

  lessons.push({
    day,
    dayRu: dayNames[day],
    time: String(formData.get("time")).trim(),
    subject: String(formData.get("subject")).trim(),
    teacher: String(formData.get("teacher")).trim(),
    room: String(formData.get("room")).trim()
  });

  form.reset();
  renderSchedule(activeDay);
});

renderSchedule(activeDay);
