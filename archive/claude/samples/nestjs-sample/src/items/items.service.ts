import { Injectable, NotFoundException } from '@nestjs/common';
import { CreateItemDto } from './dto/create-item.dto';
import { UpdateItemDto } from './dto/update-item.dto';
import { Item } from './entities/item.entity';

@Injectable()
export class ItemsService {
  // 데모용 인메모리 저장소 (실제 서비스에서는 DB 연동으로 교체)
  private items: Item[] = [];
  private nextId = 1;

  findAll(): Item[] {
    return this.items;
  }

  findOne(id: number): Item {
    const item = this.items.find((it) => it.id === id);
    if (!item) {
      throw new NotFoundException(`Item #${id} not found`);
    }
    return item;
  }

  create(dto: CreateItemDto): Item {
    const item: Item = {
      id: this.nextId++,
      name: dto.name,
      description: dto.description,
      done: dto.done ?? false,
    };
    this.items.push(item);
    return item;
  }

  update(id: number, dto: UpdateItemDto): Item {
    const item = this.findOne(id);
    Object.assign(item, dto);
    return item;
  }

  remove(id: number): void {
    const index = this.items.findIndex((it) => it.id === id);
    if (index === -1) {
      throw new NotFoundException(`Item #${id} not found`);
    }
    this.items.splice(index, 1);
  }
}
