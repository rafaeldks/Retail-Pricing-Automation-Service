function updateSheet() {
  const priceList = [
  ...
  ];
  const decisionList = ['Приоритет конкурентов', 'Текущая цена', 'Базовая маржинальность', 'Автоматизатор', 'Оптимизатор', 'Вручную'];

  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = spreadsheet.getSheetByName("main");
  const lastRow = sheet.getLastRow();

  // Установить заголовки
  sheet.getRange("Y1").setValue("base_margin_price");
  sheet.getRange("Z1").setValue("comp_price");
  sheet.getRange("AA1").setValue("manual_price");
  sheet.getRange("AB1").setValue("decision");
  sheet.getRange("AC1").setValue("new_price");

  // Установить формулы начиная со 2 строки до последней заполненной строки
  for (let i = 2; i <= lastRow; i++) {
    // Формула для столбца Y
    sheet.getRange(i, 25).setFormula(`=VLOOKUP(ROUND(F${i}/(1-G${i})), price_rounding!A:B, 2, TRUE)`);

    // Формула для столбца Z
    sheet.getRange(i, 26).setFormula(`=VLOOKUP(INDEX(J${i}:S${i}, MATCH(TRUE, J${i}:S${i} <> "", 0)), price_rounding!A:B, 2, TRUE)`);

    // Валидация данных для столбца AA
    const manualPriceRange = sheet.getRange(i, 27);
    const manualPriceRule = SpreadsheetApp.newDataValidation()
      .requireValueInList(priceList)  // Используем массив priceList
      .setAllowInvalid(false)
      .build();
    manualPriceRange.setDataValidation(manualPriceRule);

    // Валидация данных для столбца AB
    const decisionRange = sheet.getRange(i, 28);
    const decisionRule = SpreadsheetApp.newDataValidation()
      .requireValueInList(decisionList)  // Используем массив decisionList
      .setAllowInvalid(false)
      .build();
    decisionRange.setDataValidation(decisionRule);

    // Формула для столбца AC
    sheet.getRange(i, 29).setFormula(`=IFS(AB${i}="Приоритет конкурентов", Z${i}, AB${i}="Текущая цена", T${i}, AB${i}="Базовая маржинальность", Y${i}, AB${i}="Автоматизатор", U${i}, AB${i}="Оптимизатор", W${i}, AB${i}="Вручную", AA${i})`);
  }
}
