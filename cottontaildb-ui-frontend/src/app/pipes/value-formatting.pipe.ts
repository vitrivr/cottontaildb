import {Pipe, PipeTransform} from "@angular/core";
import {Type} from "../../../openapi";

/**
 * A {@link PipeTransform} that is used to format values for display.
 */
@Pipe({name: 'formatValue'})
export class ValueFormattingPipe implements PipeTransform {
  transform(value: [Type, object], exponent = 1): (string | null) {
    if (value == null) return "<NULL>";
    switch (value[0] as Type){
      case "BOOLEAN":

      case "BYTE":
      case "SHORT":
      case "INTEGER":
      case "LONG":
        return (value[1] as unknown as number).toString()
      case "FLOAT":
      case "DOUBLE":
        return (value[1] as unknown as number).toString()
      case "DATE":
        return new Date((value[1] as unknown as number)).toString()
      case "STRING":
        return (value[1] as unknown as string)
      case "COMPLEX32":
      case "COMPLEX64":
        return `${(value[1] as any).real as number} + ${(value[1] as any).imaginary as number}i`;
      case "DOUBLE_VECTOR":
      case "FLOAT_VECTOR":
      case "LONG_VECTOR":
      case "INTEGER_VECTOR":
        const c1 = value[1] as unknown as number[];
        if (c1.length < 10) {
          return `[${c1.join(",")}]`;
        } else {
          return `[${c1[0]},${c1[1]},${c1[2]}...${c1[c1.length-2]},${c1[c1.length-1]}]`;
        }
      case "BOOLEAN_VECTOR":
        const c2 = value[1] as unknown as boolean[];
        if (c2.length < 10) {
          return `[${c2.join(",")}]`;
        } else {
          return `[${c2[0]},${c2[1]},${c2[2]}...${c2[c2.length-2]},${c2[c2.length-1]}]`;
        }
      case "COMPLEX32_VECTOR":
      case "COMPLEX64_VECTOR":
        return "TODO"
      case "BYTESTRING":
        return "<BLOB>";
      case "UNDEFINED":
        return  "<NULL>";
    }
  }
}
