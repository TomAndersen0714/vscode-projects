var level = 0;
var tabspace = 4;
var maxLineLength = 70;
var timerID = null;

function fnSelect(objId) {
    fnDeSelect();
    if (document.selection) {
        var range = document.body.createTextRange();
        range.moveToElementText(document.getElementById(objId));
        range.select();
    } else if (window.getSelection) {
        var range = document.createRange();
        range.selectNode(document.getElementById(objId));
        window.getSelection().addRange(range);
    }
}

function fnDeSelect() {
    if (document.selection) document.selection.empty();
    else if (window.getSelection) window.getSelection().removeAllRanges();
}

function fmtPaste() {
    if (timerID != null) clearTimeout(timerID);
    timerID = setTimeout(sqlFormatter, 1);
}

function fmtKeyup() {
    if (timerID != null) clearTimeout(timerID);
    timerID = setTimeout(sqlFormatter, 500);
}

function finishTabifier(code) {
    code = code.replace(/\n\s*\n/g, "\n"); //blank lines
    code = code.replace(/^[\s\n]*/, ""); //leading space
    code = code.replace(/[\s\n]*$/, ""); //trailing space

    document.getElementById("formatted_code").innerHTML = jush.highlight(
        "sql",
        code
    );
    level = 0;
}

function tabs() {
    var s = "";
    for (var j = 0; j < level; j++) for (var k = 0; k < tabspace; k++) s += " ";

    return s;
}

function sqlFormatter() {
    timerID = null;
    var code = document.getElementById("sql_code").value;

    if ("\n" == code[0]) code = code.substr(1);
    //    code=code.replace(/([^\/])?\n*/g, '$1');
    //    code=code.replace(/\n\s+/g, '\n');
    code = code.replace(/[     ]+/g, " ");
    code = code.replace(/\s?([;:{},+>])\s?/g, "$1");
    code = code.replace(/\{(.*):(.*)\}/g, "{$1: $2}");

    var out = tabs(),
        li = level;
    var instring = false,
        c;
    var isNewLine = true;
    var curLineLength = 0;

    for (var i = 0; i < code.length; i++) {
        c = code.charAt(i);
        if (instring) {
            if (instring == c) {
                instring = false;
            }
            out += c;
            curLineLength++;
            isNewLine = false;
        } else if ("(" == c) {
            if ("(+)" == code.substr(i, 3)) {
                out += "(+)";
                i += 2;
                curLineLength += 3;
                isNewLine = false;
            } else {
                out += "\n" + tabs() + "(\n";
                level++;
                out += tabs();
                curLineLength = tabspace * level;
                isNewLine = true;
            }
        } else if ("/*" == code.substr(i, 2)) {
            i += 2;
            out += "/*";
            while ("*/" != code.substr(i, 2)) {
                out += code.charAt(i++);
                curLineLength++;
            }
            i++;
            out += "*/";
            curLineLength += 4;
        } else if (" from" == code.substr(i, 5).toLowerCase()) {
            out += "\n" + tabs() + code.substr(i + 1, 4);
            i += 4;
            curLineLength = tabspace * level + 4;
            isNewLine = false;
        } else if (" where" == code.substr(i, 6).toLowerCase()) {
            out += "\n" + tabs() + code.substr(i + 1, 5);
            i += 5;
            curLineLength = tabspace * level + 5;
            isNewLine = false;
        } else if (" and" == code.substr(i, 4).toLowerCase()) {
            out += "\n" + tabs() + code.substr(i + 1, 3);
            i += 3;
            curLineLength = tabspace * level + 3;
            isNewLine = false;
        } else if (" or" == code.substr(i, 3).toLowerCase()) {
            out += "\n" + tabs() + code.substr(i + 1, 2);
            i += 2;
            curLineLength = tabspace * level + 2;
            isNewLine = false;
        } else if (")" == c) {
            out = out.replace(/\s*$/, "");
            level--;
            out += "\n" + tabs() + ")"; //\n'+tabs();
            curLineLength = tabspace * level + 1;
            isNewLine = false;
        } else if ('"' == c || "'" == c) {
            if (instring && c == instring) {
                instring = false;
            } else {
                instring = c;
            }
            out += c;
            curLineLength++;
            isNewLine = false;
        } else if ("," == c) {
            out += ",\n" + tabs();
            curLineLength = tabspace * level;
            isNewLine = true;
        } else if ("\n" == c) {
            out += "\n" + tabs();
            curLineLength = tabspace * level;
            isNewLine = true;
        } else if (" " == c) {
            if (!isNewLine) {
                if (curLineLength > maxLineLength) {
                    out += "\n" + tabs();
                    isNewLine = true;
                    curLineLength = tabspace * level;
                } else out += " ";
            }
        } else {
            out += c;
            curLineLength++;
            isNewLine = false;
        }
    }
    level = li;
    out = out.replace(/[\s\n]*$/, "");
    finishTabifier(out);
}
